package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type persistentSession struct {
	pid     int
	command string
}

func runCleanup(cfg *config) int {
	if len(cfg.pos) != 0 || cfg.persist {
		fmt.Fprintln(os.Stderr, "rift: cleanup takes no source or destination and cannot be combined with --persist")
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	sessions, err := persistentSessions(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rift cleanup:", err)
		return 1
	}
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "rift cleanup:", err)
		return 1
	}
	sockets, err := sessionSockets(filepath.Join(home, ".rift"))
	if err == nil && !cfg.dryRun {
		err = stopPersistentSessions(ctx, sessions)
		if err == nil {
			for _, path := range sockets {
				if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
					err = errors.Join(err, removeErr)
				}
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "rift cleanup:", err)
		return 1
	}
	if !cfg.quiet {
		if cfg.dryRun {
			fmt.Fprintf(os.Stderr, "rift: would stop %d persistent processes and remove %d sockets\n", len(sessions), len(sockets))
		} else {
			fmt.Fprintf(os.Stderr, "rift: stopped %d persistent processes; cleaned %d sockets\n", len(sessions), len(sockets))
		}
	}
	return 0
}

// Inspect this user's processes rather than the current build's socket: an
// older agent can still be running after another build replaces agent.sock.
func persistentSessions(ctx context.Context) ([]persistentSession, error) {
	out, err := exec.CommandContext(ctx, "ps", "-U", strconv.Itoa(os.Getuid()), "-ww", "-o", "pid=,args=").Output()
	if err != nil {
		return nil, fmt.Errorf("list processes: %w", err)
	}
	return parsePersistentSessions(string(out)), nil
}

func parsePersistentSessions(out string) []persistentSession {
	var sessions []persistentSession
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		i := strings.IndexAny(line, " \t")
		if i < 0 {
			continue
		}
		pid, err := strconv.Atoi(line[:i])
		if err != nil || pid <= 1 || pid == os.Getpid() {
			continue
		}
		command := strings.TrimSpace(line[i:])
		agent := false
		if executable, ok := strings.CutSuffix(command, " __agent"); ok {
			name := filepath.Base(executable)
			agent = name == "rift" || strings.HasPrefix(name, "rift-") || name == "rift.test"
		}
		master := false
		if path, ok := strings.CutPrefix(command, "ssh: "); ok {
			if path, ok = strings.CutSuffix(path, " [mux]"); ok {
				dir := filepath.Dir(path)
				master = filepath.Base(dir) == "cm" && filepath.Base(filepath.Dir(dir)) == ".rift"
			}
		}
		if agent || master {
			sessions = append(sessions, persistentSession{pid: pid, command: command})
		}
	}
	return sessions
}

func stopPersistentSessions(ctx context.Context, sessions []persistentSession) error {
	pending := make(map[int]string, len(sessions))
	var errs error
	for _, session := range sessions {
		if err := syscall.Kill(session.pid, syscall.SIGTERM); err != nil {
			if !errors.Is(err, syscall.ESRCH) {
				errs = errors.Join(errs, fmt.Errorf("stop PID %d: %w", session.pid, err))
			}
			continue
		}
		pending[session.pid] = session.command
	}
	grace := time.NewTimer(2 * time.Second)
	defer grace.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for len(pending) > 0 {
		running, err := persistentSessions(ctx)
		if err != nil {
			return errors.Join(errs, err)
		}
		live := make(map[int]string, len(running))
		for _, session := range running {
			live[session.pid] = session.command
		}
		for pid, command := range pending {
			if live[pid] != command {
				delete(pending, pid)
			}
		}
		if len(pending) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return errors.Join(errs, fmt.Errorf("waiting for persistent processes to stop: %w", ctx.Err()))
		case <-grace.C:
			for pid := range pending {
				if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
					errs = errors.Join(errs, fmt.Errorf("kill PID %d: %w", pid, err))
				}
			}
		case <-tick.C:
		}
	}
	return errs
}

// Remove only sockets. Installed binaries, logs, and lock files survive cleanup.
func sessionSockets(dir string) ([]string, error) {
	fi, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() || fi.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%s must be a private directory (chmod 700)", dir)
	}
	paths := []string{filepath.Join(dir, "agent.sock")}
	cm := filepath.Join(dir, "cm")
	if fi, err := os.Lstat(cm); err == nil && fi.IsDir() {
		entries, err := os.ReadDir(cm)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			paths = append(paths, filepath.Join(cm, entry.Name()))
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var sockets []string
	for _, path := range paths {
		fi, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if fi.Mode()&os.ModeSocket != 0 {
			sockets = append(sockets, path)
		}
	}
	return sockets, nil
}
