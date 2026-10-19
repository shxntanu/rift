package transport

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/direct"
)

func TestOneShotSSHDisablesPersistence(t *testing.T) {
	d := NewDialer(Endpoint{Host: "test"}, Options{})
	for _, worker := range []bool{false, true} {
		args := d.sshArgs(worker, 0)
		for _, option := range []string{"ControlMaster=no", "ControlPath=none", "ControlPersist=no"} {
			if !slices.Contains(args, option) {
				t.Errorf("worker=%v: missing %s in %v", worker, option, args)
			}
		}
	}
}

func TestSSHWindowDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, log string
		want      int64
	}{
		{"upstream", "debug1: Local version string SSH-2.0-OpenSSH_10.0\ndebug1: Remote protocol version 2.0, remote software version OpenSSH_9.9\ndebug2: channel 0: open confirm rwindow 0 rmax 32768\ndebug2: channel 0: rcvd adjust 2097152\n", 2 << 20},
		{"hpn-window", "debug1: Remote protocol version 2.0, remote software version OpenSSH_HPN\ndebug2: channel 0: open confirm rwindow 33554432 rmax 32768\n", 32 << 20},
		{"hpn-unknown", "debug1: Remote protocol version 2.0, remote software version OpenSSH_HPN\n", 0},
		{"multiplexed", "debug1: auto-mux: Trying existing master\n", 2 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "ssh.log")
			if err := os.WriteFile(p, []byte(tc.log), 0600); err != nil {
				t.Fatal(err)
			}
			d := NewDialer(Endpoint{Host: "test"}, Options{})
			d.readSSHInfo(p)
			got, info := d.ChannelWindow()
			if got != tc.want {
				t.Fatalf("got %d want %d (%s)", got, tc.want, info)
			}
		})
	}
}

func TestDirectProbeFallback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	d := NewDialer(Endpoint{}, Options{Direct: true})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := d.UseDirect(ctx, &Conn{}, direct.Offer{Port: uint16(port)}); err == nil {
		t.Fatal("connected to closed port")
	}
	if d.directAddr != "" || d.control != nil {
		t.Fatal("failed probe captured SSH control or enabled direct workers")
	}
}
