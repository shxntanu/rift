package client

import (
	"fmt"
	"os"
	"testing"

	"github.com/shxntanu/rift/internal/server"
)

type testStdio struct{}

func (testStdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (testStdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (testStdio) Close() error                { return os.Stdout.Close() }

// Local transports execute the test binary with "serve", just as they do
// the rift binary. Keep the real server on the other side of the pipes.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		if err := server.Serve(testStdio{}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
