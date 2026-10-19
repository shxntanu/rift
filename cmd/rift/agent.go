package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/shxntanu/rift/internal/client"
	"golang.org/x/sys/unix"
)

const agentIdle = 15 * time.Minute

type agentRequest struct {
	Options client.Options
	Cwd     string
	Env     []string
}
type agentResponse struct {
	Log      string
	Progress *client.Progress
	Summary  *client.Summary
	Error    string
}
type agentLog struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func (l *agentLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.enc.Encode(agentResponse{Log: string(p)})
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (l *agentLog) progress(p client.Progress) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// A disconnected foreground client cancels the request's context.
	_ = l.enc.Encode(agentResponse{Progress: &p})
}

func agentDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".rift")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !fi.IsDir() || fi.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("%s must be a private directory (chmod 700)", dir)
	}
	return dir, nil
}
func dialAgent(ctx context.Context, path, build string) (net.Conn, error) {
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var got string
	err = json.NewDecoder(c).Decode(&got)
	_ = c.SetReadDeadline(time.Time{})
	if err != nil || got != build {
		c.Close()
		return nil, errors.New("agent build mismatch or unavailable")
	}
	return c, nil
}
func connectAgent(ctx context.Context) (net.Conn, error) {
	dir, err := agentDir()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "agent.sock")
	build := getBuildID()
	if c, err := dialAgent(ctx, path, build); err == nil {
		return c, nil
	}
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	log, err := os.OpenFile(filepath.Join(dir, "agent.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer log.Close()
	cmd := exec.Command(self, "__agent")
	cmd.Stdin = nil
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	started := false
	defer func() {
		if !started {
			// The agent only becomes persistent once startup succeeds.
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				// Wait will reap it if a pending kernel wait completes.
			}
		}
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-done:
			started = true // Already reaped; no startup process remains to kill.
			// A concurrent startup may have won the socket.
			if c, dialErr := dialAgent(ctx, path, build); dialErr == nil {
				return c, nil
			}
			return nil, fmt.Errorf("agent exited during startup (%v); see %s", err, filepath.Join(dir, "agent.log"))
		case <-timer.C:
			return nil, fmt.Errorf("agent startup timed out; see %s", filepath.Join(dir, "agent.log"))
		case <-ticker.C:
			if c, err := dialAgent(ctx, path, build); err == nil {
				started = true
				return c, nil
			}
		}
	}
}

func runAgentClient(ctx context.Context, opt client.Options) (*client.Summary, error) {
	c, err := connectAgent(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	if err := json.NewEncoder(c).Encode(agentRequest{Options: opt, Cwd: cwd, Env: os.Environ()}); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(c)
	for {
		var res agentResponse
		if err := dec.Decode(&res); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		if res.Log != "" {
			if _, err := io.WriteString(opt.Log, res.Log); err != nil {
				return nil, err
			}
		}
		if res.Progress != nil && opt.OnProgress != nil {
			opt.OnProgress(*res.Progress)
		}
		if res.Error != "" {
			return nil, errors.New(res.Error)
		}
		if res.Summary != nil {
			return res.Summary, nil
		}
	}
}

func serveAgent(ctx context.Context) error { return serveAgentFor(ctx, agentIdle) }

func serveAgentFor(ctx context.Context, idle time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dir, err := agentDir()
	if err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "agent.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := agentLock(ctx, lock); err != nil {
		return err
	}
	path := filepath.Join(dir, "agent.sock")
	build := getBuildID()
	if c, err := dialAgent(ctx, path, build); err == nil {
		c.Close()
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return err
	}
	// An older build may still serve its in-flight requests on an unlinked socket.
	ln.SetUnlinkOnClose(false)
	defer ln.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_UN); err != nil {
		return err
	}
	runner := client.NewRunner(ctx)
	defer runner.Close()
	gate := make(chan struct{}, 1)
	var active atomic.Int64
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	var wg sync.WaitGroup
	stop := context.AfterFunc(ctx, func() { _ = ln.Close() })
	defer stop()
	defer func() { cancel(); wg.Wait() }()
	for {
		if err := ln.SetDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		c, err := ln.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if active.Load() == 0 && time.Since(time.Unix(0, last.Load())) >= idle {
					return nil
				}
				continue
			}
			return err
		}
		active.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer active.Add(-1)
			defer func() { last.Store(time.Now().UnixNano()) }()
			defer c.Close()
			reqCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			stop := context.AfterFunc(reqCtx, func() { _ = c.Close() })
			defer stop()
			enc := json.NewEncoder(c)
			if err := enc.Encode(build); err != nil {
				return
			}
			_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
			var req agentRequest
			if err := json.NewDecoder(io.LimitReader(c, 4<<20)).Decode(&req); err != nil {
				return
			}
			_ = c.SetReadDeadline(time.Time{})
			go func() { var b [1]byte; _, _ = c.Read(b[:]); cancel() }()
			select {
			case gate <- struct{}{}:
			case <-reqCtx.Done():
				return
			}
			defer func() { <-gate }()
			// Serialize across agent builds too: replacing the socket must not
			// let an older in-flight sync race a request handled by the new binary.
			syncLock, err := os.OpenFile(filepath.Join(dir, "sync.lock"), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				_ = enc.Encode(agentResponse{Error: err.Error()})
				return
			}
			defer syncLock.Close()
			if err := agentLock(reqCtx, syncLock); err != nil {
				_ = enc.Encode(agentResponse{Error: err.Error()})
				return
			}
			// Preserve relative filter files, SSH configuration and git environment.
			if err := os.Chdir(req.Cwd); err != nil {
				_ = enc.Encode(agentResponse{Error: err.Error()})
				return
			}
			os.Clearenv()
			for _, env := range req.Env {
				for i := range env {
					if env[i] == '=' {
						_ = os.Setenv(env[:i], env[i+1:])
						break
					}
				}
			}
			logger := &agentLog{enc: enc}
			req.Options.Log = logger
			if req.Options.Rich {
				req.Options.OnProgress = logger.progress
			}

			sum, err := runner.Run(reqCtx, req.Options)
			res := agentResponse{Summary: sum}
			if err != nil {
				res.Error = err.Error()
			}
			_ = enc.Encode(res)
		}()
	}
}

func agentLock(ctx context.Context, lock *os.File) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil || (err != unix.EWOULDBLOCK && err != unix.EINTR) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
