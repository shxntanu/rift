package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/shxntanu/rift/internal/client"
)

func TestRichFlag(t *testing.T) {
	for _, args := range [][]string{{"--rich"}, {"--rich", "-vq"}, {"--rich", "--dry-run"}} {
		cfg, err := parseArgs(args)
		if err != nil || !cfg.rich {
			t.Fatalf("%v: cfg=%+v, err=%v", args, cfg, err)
		}
	}
	if _, err := parseArgs([]string{"--rich=true"}); err == nil {
		t.Fatal("rich accepted a value")
	}
}

func TestRichDisplayFitsTerminal(t *testing.T) {
	p := client.Progress{Phase: "Transferring", BytesDone: 512, BytesTotal: 1024,
		FilesDone: 1, FilesTotal: 30, Rate: 128, Elapsed: 5 * time.Second}
	for range 30 {
		p.Files = append(p.Files, client.FileProgress{Path: "file\n\x1b[2J日本語" + strings.Repeat("x", 150),
			BytesDone: 512, Total: 1024, State: "sending"})
	}
	for _, size := range [][2]int{{120, 24}, {80, 10}, {40, 8}, {10, 2}, {1, 1}} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			lines := richLines(p, size[0], size[1], newProgressModel().bar)
			if len(lines) > max(1, size[1]-1) {
				t.Fatalf("too many rows: %d", len(lines))
			}
			for _, line := range lines {
				if ansi.StringWidth(line) > max(1, size[0]-1) || strings.ContainsAny(ansi.Strip(line), "\x1b\n\r") {
					t.Fatalf("wrapping or unsafe row: %q", line)
				}
			}
			if size[0] >= 80 && !strings.Contains(strings.Join(lines, "\n"), "50.0%") {
				t.Fatal("missing total percentage")
			}
		})
	}
}

func TestRichDisplayPreservesLogsAndCleansUp(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var out bytes.Buffer
	display := newRichProgress(&out)
	display.Update(client.Progress{Phase: "Transferring"})
	if _, err := display.Write([]byte(">f example\n")); err != nil {
		t.Fatal(err)
	}
	display.Update(client.Progress{Phase: "Finishing"})
	if err := display.Close(); err != nil {
		t.Fatal(err)
	}
	end := out.Len()
	if err := display.Close(); err != nil {
		t.Fatal(err)
	}
	if out.Len() != end || !strings.Contains(ansi.Strip(out.String()), ">f example") || strings.Contains(out.String(), "38;2;") {
		t.Fatalf("lost log output, cleanup is not idempotent, or color was forced: %q", out.String())
	}
}

func TestRichModelResizeAndFinish(t *testing.T) {
	m := newProgressModel()
	resized, _ := m.Update(tea.WindowSizeMsg{Width: 40, Height: 8})
	model := resized.(progressModel)
	if model.width != 40 || model.height != 8 {
		t.Fatal("resize was ignored")
	}
	finished, cmd := model.Update(progressDoneMsg{})
	if cmd == nil || finished.View().Content != "" {
		t.Fatal("finished sync did not clear the display and quit")
	}
}
