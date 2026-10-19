// Package process runs owned subprocesses in separate Unix process groups.
package process

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// NotifyContext cancels on a shutdown signal. After the first signal,
// restore the defaults so a second signal can force a stuck shutdown.
func NotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	context.AfterFunc(ctx, stop)
	return ctx, stop
}

// CommandContext is exec.CommandContext with cancellation of the entire
// process group, including helpers spawned by SSH wrappers, git, or go build.
// Detached persistent agents and SSH masters have independent lifetimes.
func CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return Kill(cmd) }
	// Descendants can inherit output pipes even after the command exits.
	cmd.WaitDelay = 2 * time.Second
	return cmd
}

// CommandContextForeground preserves access to the controlling terminal for
// SSH password and host-key prompts. Cancellation kills its descendant tree
// instead of signaling the caller's shared foreground process group.
func CommandContextForeground(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := CommandContext(ctx, name, args...)
	cmd.SysProcAttr = nil
	return cmd
}

// Kill sends SIGKILL to a command created by this package and its children.
// Call it before Wait completes, and still call Wait to reap the command.
// Uninterruptible kernel waits can delay exit even after SIGKILL.
func Kill(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		return killForeground(cmd)
	}
	err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

func killForeground(cmd *exec.Cmd) error {
	// Stop the owner before taking the snapshot so it cannot spawn more
	// children. os.Process also rejects a PID already reaped by Wait.
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-A", "-o", "pid=,ppid=").Output()
	if err == nil {
		children := make(map[int][]int)
		for _, line := range strings.Split(string(out), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			pid, pidErr := strconv.Atoi(fields[0])
			ppid, ppidErr := strconv.Atoi(fields[1])
			if pidErr == nil && ppidErr == nil && pid > 1 {
				children[ppid] = append(children[ppid], pid)
			}
		}
		var killChildren func(int)
		killChildren = func(parent int) {
			for _, pid := range children[parent] {
				killChildren(pid)
				_ = syscall.Kill(pid, syscall.SIGKILL) // A child may already have exited.
			}
		}
		killChildren(cmd.Process.Pid)
	}
	// Always kill the owner, including when ps fails or times out.
	return errors.Join(err, cmd.Process.Kill())
}
