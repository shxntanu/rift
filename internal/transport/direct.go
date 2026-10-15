package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/shxntanu/rift/internal/direct"
	"github.com/shxntanu/rift/internal/proto"
)

// UseDirect retains the SSH control session for the listener's lifetime. A
// failed initial probe leaves the existing SSH connection fully usable.
func (d *Dialer) UseDirect(ctx context.Context, control *Conn, offer direct.Offer) (*Conn, error) {
	host := "127.0.0.1"
	if d.ep.Host != "" {
		// Respect SSH Host aliases and HostName without guessing from user@host.
		args := append([]string{"-G"}, d.opt.SSHArgs...)
		args = append(args, d.ep.Host)
		out, err := exec.CommandContext(ctx, d.opt.SSH, args...).Output()
		if err != nil {
			return nil, fmt.Errorf("resolve direct SSH host: %w", err)
		}
		host = ""
		for _, line := range strings.Split(string(out), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 && f[0] == "hostname" {
				host = f[1]
				break
			}
		}
		if host == "" {
			return nil, fmt.Errorf("SSH config has no hostname")
		}
	}
	d.directAddr = net.JoinHostPort(host, strconv.Itoa(int(offer.Port)))
	d.directOffer = offer
	c, err := d.dialDirect(ctx, 0)
	if err != nil {
		d.directAddr = ""
		return nil, err
	}
	d.control = control
	return c, nil
}

func (d *Dialer) dialDirect(ctx context.Context, slot int) (*Conn, error) {
	nd := net.Dialer{Timeout: direct.Timeout}
	raw, err := nd.DialContext(ctx, "tcp", d.directAddr)
	if err != nil {
		return nil, err
	}
	raw = &countedTCP{Conn: raw, d: d}
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	secure, err := direct.Client(raw, d.directOffer, slot)
	if err == nil {
		var magic [len(proto.Magic)]byte
		// Bound the protocol greeting as well as authentication.
		_ = raw.SetReadDeadline(time.Now().Add(direct.Timeout))
		_, err = io.ReadFull(secure, magic[:])
		if err == nil && string(magic[:]) != proto.Magic {
			err = fmt.Errorf("invalid direct greeting")
		}
		if err == nil {
			err = raw.SetReadDeadline(time.Time{})
		}
	}
	if err != nil {
		stop()
		_ = raw.Close()
		return nil, err
	}
	return &Conn{stdin: secure, stdout: secure, d: d, stop: stop}, nil
}

// Close releases the SSH control process after all direct data streams close.
func (d *Dialer) Close() error {
	if d.control != nil {
		return d.control.Close()
	}
	return nil
}

// Count ciphertext, including record overhead and authentication, rather than
// the decrypted bytes returned by the direct stream.
type countedTCP struct {
	net.Conn
	d *Dialer
}

func (c *countedTCP) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.d.WireIn.Add(int64(n))
	return n, err
}
func (c *countedTCP) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.d.WireOut.Add(int64(n))
	return n, err
}
func (c *countedTCP) CloseWrite() error { return c.Conn.(*net.TCPConn).CloseWrite() }
