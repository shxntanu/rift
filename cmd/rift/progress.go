package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"charm.land/bubbles/v2/progress"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/charmbracelet/x/ansi"
	"github.com/shxntanu/rift/internal/client"
)

// richProgress keeps terminal rendering in the foreground, including when an
// agent performs the transfer. Bubble Tea serializes logs and snapshots.
type richProgress struct {
	program *tea.Program
	done    chan error
	once    sync.Once
	err     error
}

func newRichProgress(out io.Writer) *richProgress {
	opts := []tea.ProgramOption{tea.WithOutput(out), tea.WithInput(nil), tea.WithoutSignalHandler()}
	if os.Getenv("NO_COLOR") != "" {
		opts = append(opts, tea.WithColorProfile(colorprofile.ASCII))
	}
	r := &richProgress{program: tea.NewProgram(newProgressModel(), opts...), done: make(chan error, 1)}
	go func() {
		_, err := r.program.Run()
		r.done <- err
	}()
	return r
}

func (r *richProgress) Update(p client.Progress) {
	r.program.Send(p)
}

func (r *richProgress) Write(p []byte) (int, error) {
	r.program.Println(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

func (r *richProgress) Close() error {
	r.once.Do(func() {
		r.program.Send(progressDoneMsg{})
		r.err = <-r.done
	})
	return r.err
}

type progressDoneMsg struct{}

type progressModel struct {
	progress      client.Progress
	width, height int
	bar           progress.Model
	quitting      bool
}

func newProgressModel() progressModel {
	return progressModel{width: 80, height: 24,
		progress: client.Progress{Phase: "Starting sync"},
		bar:      progress.New(progress.WithColors(lipgloss.Color("#ff7821")), progress.WithoutPercentage()),
	}
}

func (m progressModel) Init() tea.Cmd { return nil }

func (m progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
	case client.Progress:
		m.progress = msg
	case progressDoneMsg:
		m.quitting = true
		return m, tea.Quit
	}
	return m, nil
}

func (m progressModel) View() tea.View {
	if m.quitting {
		return tea.NewView("")
	}
	return tea.NewView(strings.Join(richLines(m.progress, m.width, m.height, m.bar), "\n"))
}

func progressPercent(done, total int64, complete bool) float64 {
	if total == 0 {
		if complete {
			return 100
		}
		return 0
	}
	return max(0, min(100, 100*float64(done)/float64(total)))
}

func richLines(p client.Progress, width, height int, bar progress.Model) []string {
	width = max(1, width-1) // Leave the last column unused to avoid wrapping.
	pct := progressPercent(p.BytesDone, p.BytesTotal,
		p.FilesDone == p.FilesTotal && (p.Phase == "Transferring" || p.Phase == "Finishing"))
	elapsed := p.Elapsed.Truncate(time.Second)
	bar.SetWidth(min(32, max(4, width/3)))
	lines := []string{
		lipgloss.NewStyle().Bold(true).Render(fmt.Sprintf("rift · %s · %s elapsed", p.Phase, elapsed)),
		fmt.Sprintf("Total %s %5.1f%%  %s / %s", bar.ViewAs(pct/100), pct,
			client.HumanBytes(p.BytesDone), client.HumanBytes(p.BytesTotal)),
		fmt.Sprintf("Files %d/%d | %s/s | wire %s | %d conns", p.FilesDone, p.FilesTotal,
			client.HumanBytes(int64(p.Rate)), client.HumanBytes(p.WireOut), p.Conns),
	}
	if p.Failed > 0 {
		lines[2] += fmt.Sprintf(" | %d failed", p.Failed)
	}
	if p.BytesTotal > p.BytesDone && p.Rate > 0 {
		eta := time.Duration(float64(p.BytesTotal-p.BytesDone)/p.Rate) * time.Second
		lines[2] += " | ETA " + eta.String()
	}
	rows := max(0, height-5)
	bar.SetWidth(10)
	for _, f := range p.Files[:min(rows, len(p.Files))] {
		pct := progressPercent(f.BytesDone, f.Total, f.State == "done")
		// Escape terminal controls while preserving Unicode file names.
		name := strconv.Quote(f.Path)
		name = name[1 : len(name)-1]
		fileBar := bar.ViewAs(pct / 100)
		if width < 60 {
			fileBar = ""
		}
		tail := fmt.Sprintf(" %s %5.1f%% %-8s", fileBar, pct, f.State)
		if width >= 100 {
			tail += fmt.Sprintf(" %s / %s", client.HumanBytes(f.BytesDone), client.HumanBytes(f.Total))
		}
		nameWidth := max(1, width-ansi.StringWidth(tail))
		name = lipgloss.NewStyle().Width(nameWidth).Render(ansi.Truncate(name, nameWidth, "…"))
		lines = append(lines, name+tail)
	}
	if hidden := p.FilesTotal - min(rows, len(p.Files)) - p.FilesDone; hidden > 0 {
		lines = append(lines, fmt.Sprintf("... %d more unfinished files", hidden))
	}
	lines = lines[:min(len(lines), max(1, height-1))]
	for i := range lines {
		lines[i] = ansi.Truncate(lines[i], width, "…")
	}
	return lines
}
