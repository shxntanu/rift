// Package transport spawns rift server processes, locally or over SSH, and
// deploys the rift binary to remote hosts on first use.
package transport

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shxntanu/rift/internal/direct"
	"github.com/shxntanu/rift/internal/process"
	"github.com/shxntanu/rift/internal/proto"
)

// Endpoint is a sync destination: a local path or host:path.
type Endpoint struct {
	Host string // empty for local
	Path string
}

// ParseEndpoint splits "[user@]host:path". A colon that appears after a
// slash is part of a local path.
func ParseEndpoint(s string) Endpoint {
	i := strings.IndexByte(s, ':')
	j := strings.IndexByte(s, '/')
	if i > 0 && (j < 0 || i < j) {
		return Endpoint{Host: s[:i], Path: s[i+1:]}
	}
	return Endpoint{Path: s}
}

func (e Endpoint) String() string {
	if e.Host == "" {
		return e.Path
	}
	return e.Host + ":" + e.Path
}

// Options configure how server processes are started.
type Options struct {
	Direct     bool     // opt-in SSH-bootstrapped encrypted TCP
	SSH        string   // ssh binary
	SSHArgs    []string // extra args inserted before the host
	FastCipher bool     // prefer AES-GCM, which is hardware accelerated on most CPUs
	BuildID    string   // identifies the exact rift build; names the remote binary
	// Persist keeps every connection's ssh session alive in the background
	// (ControlMaster/ControlPersist) so the next sync skips TCP and key
	// exchange handshakes entirely.
	Persist    bool
	PersistFor string // ControlPersist value, e.g. "15m"
}

// Dialer opens connections to server processes for one endpoint.
type Dialer struct {
	ep  Endpoint
	opt Options

	localSSH, remoteSSH string
	sshWindow           int64
	deployMu            sync.Mutex
	deployed            bool
	directAddr          string
	directOffer         direct.Offer
	control             *Conn

	// WireOut / WireIn count raw bytes crossing the transport.
	WireOut atomic.Int64
	WireIn  atomic.Int64
}

// NewDialer returns a dialer for ep.
func NewDialer(ep Endpoint, opt Options) *Dialer {
	if opt.SSH == "" {
		opt.SSH = "ssh"
	}
	return &Dialer{ep: ep, opt: opt}
}

// Conn carries the protocol over a server process or an encrypted TCP stream.
type Conn struct {
	stop   func() bool
	cancel context.CancelFunc
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr *stderrBuffer
	d      *Dialer
	once   sync.Once
	err    error
}

func (c *Conn) Read(p []byte) (int, error) {
	n, err := c.stdout.Read(p)
	if c.cmd != nil {
		c.d.WireIn.Add(int64(n))
	}
	return n, err
}

func (c *Conn) Write(p []byte) (int, error) {
	n, err := c.stdin.Write(p)
	if c.cmd != nil {
		c.d.WireOut.Add(int64(n))
	}
	return n, err
}

// CloseWrite signals EOF to the server.
func (c *Conn) CloseWrite() error {
	if cw, ok := c.stdin.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return c.stdin.Close()
}

// CloseRead interrupts reads without waiting for the server process to exit.
func (c *Conn) CloseRead() error { return c.stdout.Close() }

// Close signals EOF to the server and waits briefly for it to exit.
func (c *Conn) Close() error {
	c.once.Do(func() {
		if c.cancel != nil {
			defer c.cancel()
		}
		if c.stop != nil {
			defer c.stop()
		}
		if c.cmd == nil {
			c.err = c.stdout.Close()
			return
		}
		_ = c.stdin.Close()
		done := make(chan error, 1)
		go func() { done <- c.cmd.Wait() }()
		select {
		case c.err = <-done:
		case <-time.After(10 * time.Second):
			killErr := process.Kill(c.cmd)
			select {
			case c.err = <-done:
			case <-time.After(2 * time.Second):
				// Keep Wait running so the child is reaped if the kernel
				// eventually releases it, but don't hang shutdown forever.
				c.err = errors.Join(killErr, fmt.Errorf("PID %d did not exit after SIGKILL; it may be blocked in the kernel", c.cmd.Process.Pid))
			}
		}
		_ = c.stdout.Close()
	})
	return c.err
}

// A process stuck in the kernel can outlive Close's deadline. Synchronize
// stderr reads with exec's copy goroutine even in that case.
type stderrBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *stderrBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *stderrBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Stderr returns whatever the process wrote to stderr (worker connections only).
func (c *Conn) Stderr() string {
	if c.stderr == nil {
		return ""
	}
	return strings.TrimSpace(c.stderr.String())
}

const remoteDir = "$HOME/.rift/bin"

func (d *Dialer) remoteBin() string { return remoteDir + "/rift-" + d.opt.BuildID }

func (d *Dialer) sshArgs(worker bool, idx int) []string {
	// Like scp, drop any forwardings from the user's config: several parallel
	// connections must not fight over the same forwarded sockets.
	args := []string{"-T", "-x", "-a", "-o", "ClearAllForwardings=yes", "-o", "PermitLocalCommand=no",
		"-o", "Compression=no", "-o", "ServerAliveInterval=15"}
	if d.opt.FastCipher {
		args = append(args, "-o", "Ciphers=^aes128-gcm@openssh.com,aes256-gcm@openssh.com")
	}
	// Every connection gets its own control socket: they must stay
	// independent TCP connections (separate congestion windows and separate
	// ssh crypto processes), since one shared master would funnel all
	// traffic through a single stream.
	if dir := d.controlDir(); dir != "" {
		args = append(args,
			"-o", "ControlMaster=auto",
			"-o", fmt.Sprintf("ControlPath=%s/%%C-%d", dir, idx),
			"-o", "ControlPersist="+d.opt.PersistFor)
	} else {
		args = append(args, "-o", "ControlMaster=no", "-o", "ControlPath=none", "-o", "ControlPersist=no")
	}
	if worker {
		// Interactive auth happens once, on the primary connection.
		args = append(args, "-o", "BatchMode=yes")
	}
	return append(args, d.opt.SSHArgs...)
}

func (d *Dialer) controlDir() string {
	if !d.opt.Persist {
		return ""
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	dir := filepath.Join(home, ".rift", "cm")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return ""
	}
	return dir
}

// ErrNeedDeploy is returned by worker dials when the remote lacks a binary.
var ErrNeedDeploy = errors.New("rift is not installed on the remote")

// DialRetained respects ctx during startup, then keeps a successful connection
// alive until Close or lifetime cancellation, for reuse across requests.
func (d *Dialer) DialRetained(ctx, lifetime context.Context, worker bool, idx int) (*Conn, error) {
	owned, cancel := context.WithCancel(lifetime)
	stop := context.AfterFunc(ctx, cancel)
	c, err := d.Dial(owned, worker, idx)
	stop()
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		cancel()
		if c != nil {
			_ = c.Close()
		}
		return nil, err
	}
	c.cancel = cancel
	return c, nil
}

// Dial starts a server process. The primary connection (worker=false) may
// prompt for credentials and deploys the binary if needed.
func (d *Dialer) Dial(ctx context.Context, worker bool, idx int) (*Conn, error) {
	if worker && d.directAddr != "" {
		if c, err := d.dialDirect(ctx, idx); err == nil {
			return c, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	c, need, err := d.start(ctx, worker, idx)
	if err != nil {
		return nil, err
	}
	if need == "" {
		return c, nil
	}
	if worker {
		return nil, ErrNeedDeploy
	}
	if err := d.deploy(ctx, need); err != nil {
		return nil, err
	}
	c, need, err = d.start(ctx, worker, idx)
	if err != nil {
		return nil, err
	}
	if need != "" {
		return nil, errors.New("remote still reports rift missing after deploy")
	}
	return c, nil
}

func (d *Dialer) command(ctx context.Context, worker bool, idx int, debugFile string) (*exec.Cmd, error) {
	if d.ep.Host == "" {
		self, err := os.Executable()
		if err != nil {
			return nil, err
		}
		return process.CommandContext(ctx, self, "serve"), nil
	}
	script := fmt.Sprintf(`p="%s"; if [ -x "$p" ]; then exec "$p" serve; fi; echo "RIFT-NEED $(uname -s) $(uname -m)"`, d.remoteBin())
	args := d.sshArgs(worker, idx)
	if debugFile != "" {
		args = append(args, "-vv", "-E", debugFile)
	}
	args = append(args, d.ep.Host, "sh -c '"+script+"'")
	if !worker {
		return process.CommandContextForeground(ctx, d.opt.SSH, args...), nil
	}
	return process.CommandContext(ctx, d.opt.SSH, args...), nil
}

// start launches the process and reads the server magic. If the remote
// lacks the binary it returns the reported "OS ARCH" in need.
func (d *Dialer) start(ctx context.Context, worker bool, idx int) (*Conn, string, error) {
	debugFile := ""
	if !worker && d.ep.Host != "" {
		if f, err := os.CreateTemp("", "rift-ssh-*.log"); err == nil {
			debugFile = f.Name()
			_ = f.Close()
			defer func() { _ = os.Remove(debugFile) }()
		}
	}
	cmd, err := d.command(ctx, worker, idx, debugFile)
	if err != nil {
		return nil, "", err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, "", err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, "", err
	}
	c := &Conn{cmd: cmd, stdin: stdin, stdout: stdout, d: d}
	if worker {
		c.stderr = &stderrBuffer{}
		cmd.Stderr = c.stderr
	} else {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, "", fmt.Errorf("start %s: %w", cmd.Path, err)
	}
	// Closing pipes also unblocks a greeting read when a killed wrapper's
	// descendants inherited them, or when the child is stuck in the kernel.
	stop := context.AfterFunc(ctx, func() { _ = stdin.Close(); _ = stdout.Close() })
	c.stop = stop
	var magic [5]byte
	if _, err := io.ReadFull(stdout, magic[:]); err != nil {
		_ = c.Close()
		msg := c.Stderr()
		if msg == "" && debugFile != "" {
			msg = sshFailure(debugFile)
		}
		if msg != "" {
			return nil, "", fmt.Errorf("connect to %s: %s", d.ep, msg)
		}
		return nil, "", fmt.Errorf("connect to %s: %w", d.ep, err)
	}
	switch string(magic[:]) {
	case proto.Magic:
		if debugFile != "" {
			d.readSSHInfo(debugFile)
		}
		return c, "", nil
	case "RIFT-":
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		_ = c.Close()
		return nil, strings.TrimSpace(strings.TrimPrefix(line, "NEED")), nil
	default:
		_ = c.Close()
		return nil, "", fmt.Errorf("connect to %s: unexpected output %q (does your shell print to"+
			"stdout on login?)", d.ep, magic[:])
	}
}

func goTarget(unameOS, unameArch string) (string, string, error) {
	var goos, goarch string
	switch strings.ToLower(unameOS) {
	case "linux":
		goos = "linux"
	case "darwin":
		goos = "darwin"
	case "freebsd":
		goos = "freebsd"
	default:
		return "", "", fmt.Errorf("unsupported remote OS %q", unameOS)
	}
	switch unameArch {
	case "x86_64", "amd64":
		goarch = "amd64"
	case "aarch64", "arm64":
		goarch = "arm64"
	case "armv7l", "armv6l":
		goarch = "arm"
	case "riscv64":
		goarch = "riscv64"
	default:
		return "", "", fmt.Errorf("unsupported remote arch %q", unameArch)
	}
	return goos, goarch, nil
}

// SourceDir is set at build time so the client can cross-compile a server
// binary for remotes whose platform has no prebuilt sibling binary.
var SourceDir string

// binaryFor finds or builds a rift binary for goos/goarch.
func (d *Dialer) binaryFor(ctx context.Context, goos, goarch string) (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}
	if goos == runtime.GOOS && goarch == runtime.GOARCH {
		return self, nil
	}
	name := "rift-" + goos + "-" + goarch
	sibling := filepath.Join(filepath.Dir(self), name)
	if _, err := os.Stat(sibling); err == nil {
		return sibling, nil
	}
	src := os.Getenv("RIFT_SRC")
	if src == "" {
		src = SourceDir
	}
	gobin, lookErr := exec.LookPath("go")
	if src == "" || lookErr != nil {
		return "", fmt.Errorf("no rift binary for %s/%s: put %s next to %s (make cross) or "+
			"set RIFT_SRC to the source tree", goos, goarch, name, self)
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	out := filepath.Join(cache, "rift", name+"-"+d.opt.BuildID)
	if _, err := os.Stat(out); err == nil {
		return out, nil
	}
	fmt.Fprintf(os.Stderr, "rift: cross-compiling server for %s/%s...\n", goos, goarch)
	cmd := process.CommandContext(ctx, gobin, "build", "-trimpath", "-ldflags=-s -w", "-o", out,
		"./cmd/rift")
	cmd.Dir = src
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("cross-compile: %w", err)
	}
	return out, nil
}

func (d *Dialer) deploy(ctx context.Context, need string) error {
	d.deployMu.Lock()
	defer d.deployMu.Unlock()
	if d.deployed {
		return nil
	}
	f := strings.Fields(need)
	if len(f) != 2 {
		return fmt.Errorf("cannot parse remote platform %q", need)
	}
	goos, goarch, err := goTarget(f[0], f[1])
	if err != nil {
		return err
	}
	bin, err := d.binaryFor(ctx, goos, goarch)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(bin)
	if err != nil {
		return err
	}
	var gz bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&gz, gzip.BestSpeed)
	if _, err := zw.Write(data); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "rift: installing server on %s (%s/%s, %.1f MiB)\n", d.ep.Host, goos,
		goarch, float64(gz.Len())/(1<<20))
	tmp := remoteDir + "/.rift-" + d.opt.BuildID + ".$$"
	script := fmt.Sprintf(`mkdir -p "%s" && gzip -dc > "%s" && chmod 755 "%s" && mv -f "%s" "%s"`,
		remoteDir, tmp, tmp, tmp, d.remoteBin())
	args := append(d.sshArgs(false, 0), d.ep.Host, "sh -c '"+script+"'")
	cmd := process.CommandContextForeground(ctx, d.opt.SSH, args...)
	cmd.Stdin = &gz
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("deploy to %s: %w", d.ep.Host, err)
	}
	d.deployed = true
	return nil
}
