package transport

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// OpenSSH exposes the actual peer channel window only in debug diagnostics,
// not through ssh -G or a version query. Capture those diagnostics separately
// from interactive authentication, without printing them or any credentials.
var channelWindow = regexp.MustCompile(`channel [0-9]+: (?:open confirm rwindow |rcvd adjust )([0-9]+)`)

func (d *Dialer) readSSHInfo(file string) {
	data, err := os.ReadFile(file)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		if _, v, ok := strings.Cut(line, "Local version string "); ok {
			d.localSSH = strings.TrimSpace(v)
		}
		if _, v, ok := strings.Cut(line, "remote software version "); ok {
			d.remoteSSH = strings.TrimSpace(v)
		}
		if m := channelWindow.FindStringSubmatch(line); m != nil {
			n, err := strconv.ParseInt(m[1], 10, 64)
			if err == nil && n > 0 && d.sshWindow == 0 {
				d.sshWindow = n
			}
		}
	}
}

// ChannelWindow returns the observed receiving window and its provenance.
// Multiplexed sessions and non-OpenSSH clients may not expose a window; the
// upstream 2 MiB default is explicitly an estimate in that case.
func (d *Dialer) ChannelWindow() (int64, string) {
	if d.ep.Host == "" || d.directAddr != "" {
		return 0, "local/direct transport; no SSH channel window"
	}
	versions := fmt.Sprintf("local=%q remote=%q", d.localSSH, d.remoteSSH)
	if d.sshWindow > 0 {
		return d.sshWindow, fmt.Sprintf("observed SSH window=%d (%s)", d.sshWindow, versions)
	}
	if strings.Contains(strings.ToLower(d.localSSH+d.remoteSSH), "hpn") {
		return 0, "HPN SSH: window unobserved; skipping fixed-window sizing (" + versions + ")"
	}
	return 2 << 20, "assuming upstream 2 MiB SSH window; diagnostics unavailable (" + versions + ")"
}

func sshFailure(file string) string {
	data, err := os.ReadFile(file)
	if err != nil {
		return ""
	}
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "debug1:") && !strings.HasPrefix(line, "debug2:") && !strings.HasPrefix(line, "debug3:") {
			lines = append(lines, line)
		}
	}
	if len(lines) > 20 {
		lines = lines[len(lines)-20:]
	}
	return strings.Join(lines, "\n")
}
