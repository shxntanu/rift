//go:build ignore

// gen renders rift's pixel-art logo from the bitmaps below.
//
//	go run assets/gen.go
//
// It writes an SVG and a PNG for documentation, and an ANSI version that
// draws two pixels per terminal cell with half-block characters.
package main

import (
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strings"
)

// Glyphs on a 2px stroke grid: 4 rows of ascender, 8 rows of x-height.
var glyphs = map[rune][]string{
	'r': {
		"......",
		"......",
		"......",
		"......",
		"##.###",
		"######",
		"###...",
		"##....",
		"##....",
		"##....",
		"##....",
		"##....",
	},
	'i': {
		"..",
		"##",
		"##",
		"..",
		"##",
		"##",
		"##",
		"##",
		"##",
		"##",
		"##",
		"##",
	},
	'f': {
		"..###",
		".####",
		".##..",
		".##..",
		"#####",
		"#####",
		".##..",
		".##..",
		".##..",
		".##..",
		".##..",
		".##..",
	},
	't': {
		".....",
		".##..",
		".##..",
		".##..",
		"#####",
		"#####",
		".##..",
		".##..",
		".##..",
		".##..",
		".####",
		"..###",
	},
}

type px struct {
	c color.NRGBA
	a float64 // opacity, 0 = empty
}

type canvas struct {
	w, h int
	p    [][]px
}

func newCanvas(w, h int) *canvas {
	c := &canvas{w: w, h: h, p: make([][]px, h)}
	for y := range c.p {
		c.p[y] = make([]px, w)
	}
	return c
}

func (c *canvas) set(x, y int, col color.NRGBA, a float64) {
	if x < 0 || y < 0 || x >= c.w || y >= c.h {
		return
	}
	c.p[y][x] = px{col, a}
}

func word(s string) (mask [][]bool, w int) {
	const gap = 2
	h := 12
	mask = make([][]bool, h)
	for _, r := range s {
		g := glyphs[r]
		gw := len(g[0])
		for y := range h {
			for x := range gw {
				mask[y] = append(mask[y], g[y][x] == '#')
			}
			for range gap {
				mask[y] = append(mask[y], false)
			}
		}
		w += gw + gap
	}
	w -= gap
	for y := range mask {
		mask[y] = mask[y][:w]
	}
	return mask, w
}

func hex(s string) color.NRGBA {
	var r, g, b uint8
	fmt.Sscanf(strings.TrimPrefix(s, "#"), "%02x%02x%02x", &r, &g, &b)
	return color.NRGBA{r, g, b, 255}
}

func lerp(a, b color.NRGBA, t float64) color.NRGBA {
	f := func(x, y uint8) uint8 { return uint8(float64(x) + (float64(y)-float64(x))*t + 0.5) }
	return color.NRGBA{f(a.R, b.R), f(a.G, b.G), f(a.B, b.B), 255}
}

func gradient(stops []color.NRGBA, t float64) color.NRGBA {
	if len(stops) == 1 {
		return stops[0]
	}
	t = max(0, min(1, t))
	seg := t * float64(len(stops)-1)
	i := min(int(seg), len(stops)-2)
	return lerp(stops[i], stops[i+1], seg-float64(i))
}

type theme struct {
	word                []color.NRGBA // stops across the word, left to right
	stream              color.NRGBA   // stream lines next to the word
	streamIn, streamOut color.NRGBA   // stream lines entering and leaving the tear
	tear                []color.NRGBA // light in the tear, top to bottom
	core                color.NRGBA   // hottest centre of the gap
	tile, rim           color.NRGBA
}

var themes = map[string]theme{
	"ember": {
		word:     []color.NRGBA{hex("#ffb627"), hex("#ff5d1f")},
		stream:   hex("#ffb627"),
		streamIn: hex("#ffb627"), streamOut: hex("#ff5d1f"),
		tear: []color.NRGBA{hex("#ffc94d"), hex("#ff8a1f"), hex("#f2461b")},
		core: hex("#fff4d6"),
		tile: hex("#1c1917"), rim: hex("#2e2925"),
	},
	"phosphor": {
		word:     []color.NRGBA{hex("#5cff6a")},
		stream:   hex("#5cff6a"),
		streamIn: hex("#5cff6a"), streamOut: hex("#5cff6a"),
		tear: []color.NRGBA{hex("#a8ffad"), hex("#3fdc55"), hex("#1f9e3a")},
		core: hex("#f0fff0"),
		tile: hex("#0f1712"), rim: hex("#1d2a21"),
	},
	"signal": {
		word:     []color.NRGBA{hex("#ece7df")},
		stream:   hex("#ff5d1f"),
		streamIn: hex("#ece7df"), streamOut: hex("#ff5d1f"),
		tear: []color.NRGBA{hex("#ff7a2e"), hex("#ff5d1f"), hex("#e0401a")},
		core: hex("#fff1e0"),
		tile: hex("#18181b"), rim: hex("#2a2a2e"),
	},
}

var th = themes["ember"]

// wordmark draws "rift" with a left-to-right gradient. Stream lines of
// different lengths run into it along the x-height, one per terminal row:
// parallel connections, each having carried a different share.
func wordmark(streams []int) *canvas {
	mask, w := word("rift")
	lead := 0
	for _, n := range streams {
		lead = max(lead, n+2)
	}
	c := newCanvas(lead+w, len(mask))
	for y := range mask {
		for x := range w {
			if mask[y][x] {
				c.set(lead+x, y, gradient(th.word, float64(x)/float64(w-1)), 1)
			}
		}
	}
	for i, n := range streams {
		y := 4 + 2*i
		for k := range n {
			// solid near the word, dashed and fading further out
			if k > n/2 && k%2 == 1 {
				continue
			}
			c.set(lead-2-k, y, th.stream, 1-0.8*float64(k)/float64(n))
		}
	}
	return c
}

// blend paints col over whatever is already at (x, y).
func (c *canvas) blend(x, y int, col color.NRGBA, a float64) {
	if x < 0 || y < 0 || x >= c.w || y >= c.h {
		return
	}
	if p := c.p[y][x]; p.a == 1 {
		c.p[y][x] = px{lerp(p.c, col, a), 1}
		return
	}
	c.set(x, y, col, a)
}

// portal is the square mark: a solid tile torn open top to bottom, light
// pouring out of the gap. With streams, lines of data cross the tear,
// cyan going in and pink coming out.
func portal(streams, shift bool) *canvas {
	const size = 24
	c := newCanvas(size, size)
	tile, rim := th.tile, th.rim
	for y := 1; y < size-1; y++ {
		for x := 1; x < size-1; x++ {
			// rounded corners
			dx, dy := min(x-1, size-2-x), min(y-1, size-2-y)
			if dx+dy < 2 {
				continue
			}
			col := tile
			if dx == 0 || dy == 0 || dx+dy == 2 {
				col = rim
			}
			c.set(x, y, col, 1)
		}
	}

	// The tear: left edge and width per row, narrow at the ends and gaping
	// in the middle, zigzagging like a rip.
	left := []int{12, 12, 11, 10, 10, 9, 11, 12, 12, 10, 9, 9, 11, 13, 13, 12, 11, 12, 11, 10, 11, 12}
	width := []int{1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 4, 4, 4, 3, 3, 3, 2, 2, 2, 1, 1}

	// The right half has slipped down a pixel, so the rim no longer lines up
	// across the tear.
	if shift {
		edge := func(y int) int { return left[max(0, min(y-1, len(left)-1))] }
		for y := size - 1; y > 0; y-- {
			for x := edge(y); x < size; x++ {
				c.p[y][x] = c.p[y-1][x]
			}
		}
		for x := edge(0); x < size; x++ {
			c.p[0][x] = px{}
		}
	}
	for i := range left {
		y := i + 1
		l, w := left[i], width[i]
		col := gradient(th.tear, float64(i)/float64(len(left)-1))
		for k := range w {
			// hottest in the middle of the gap
			mid := float64(w-1) / 2
			t := 1 - abs(float64(k)-mid)/max(mid+0.5, 1)
			c.set(l+k, y, lerp(col, th.core, 0.6*t), 1)
		}
		for d, a := range []float64{0.6, 0.25} {
			c.blend(l-1-d, y, col, a)
			c.blend(l+w+d, y, col, a)
		}
	}

	if streams {
		for _, y := range []int{7, 12, 17} {
			i := y - 1
			gapL, gapR := left[i]-3, left[i]+width[i]+2
			for x := 3; x <= gapL; x++ {
				k := gapL - x
				if k > 3 && k%2 == 1 {
					continue
				}
				c.blend(x, y, th.streamIn, 1-0.75*float64(k)/float64(gapL-3+1))
			}
			for x := gapR; x <= size-4; x++ {
				k := x - gapR
				if k > 3 && k%2 == 1 {
					continue
				}
				c.blend(x, y, th.streamOut, 1-0.75*float64(k)/float64(size-4-gapR+1))
			}
		}
	}
	return c
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// blit copies src onto dst at (ox, oy).
func blit(dst, src *canvas, ox, oy int) {
	for y := range src.h {
		for x := range src.w {
			if p := src.p[y][x]; p.a > 0 {
				dst.set(ox+x, oy+y, p.c, p.a)
			}
		}
	}
}

// lockup puts the portal next to the plain wordmark. The word sits on an
// even row so its 2px bands line up with terminal cells.
func lockup() *canvas {
	icon, word := portal(true, true), wordmark(nil)
	c := newCanvas(icon.w+3+word.w, icon.h)
	blit(c, icon, 0, 0)
	blit(c, word, icon.w+3, (icon.h-word.h)/2&^1)
	return c
}

func (c *canvas) png(path string, scale int, bg *color.NRGBA) error {
	img := image.NewNRGBA(image.Rect(0, 0, c.w*scale, c.h*scale))
	for y := range c.h {
		for x := range c.w {
			p := c.p[y][x]
			var col color.NRGBA
			switch {
			case p.a > 0 && bg != nil:
				col = lerp(*bg, p.c, p.a)
			case p.a > 0:
				col = p.c
				col.A = uint8(p.a*255 + 0.5)
			case bg != nil:
				col = *bg
			default:
				continue
			}
			for dy := range scale {
				for dx := range scale {
					img.SetNRGBA(x*scale+dx, y*scale+dy, col)
				}
			}
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return png.Encode(f, img)
}

// svg emits one rect per horizontal run of identical pixels, with
// crispEdges so it stays sharp at any size.
func (c *canvas) svg(path string) error {
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" shape-rendering="crispEdges">`+"\n", c.w, c.h)
	for y := range c.h {
		for x := 0; x < c.w; {
			p := c.p[y][x]
			if p.a == 0 {
				x++
				continue
			}
			n := 1
			for x+n < c.w && c.p[y][x+n] == p {
				n++
			}
			op := ""
			if p.a < 1 {
				op = fmt.Sprintf(` fill-opacity="%.2f"`, p.a)
			}
			fmt.Fprintf(&b, `<rect x="%d" y="%d" width="%d" height="1" fill="#%02x%02x%02x"%s/>`+"\n",
				x, y, n, p.c.R, p.c.G, p.c.B, op)
			x += n
		}
	}
	b.WriteString("</svg>\n")
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// ansi draws two pixels per cell: "▀" with the top pixel as foreground and
// the bottom one as background. Translucent pixels are blended towards a
// dark terminal background.
func (c *canvas) ansi() string {
	dark := color.NRGBA{0x10, 0x10, 0x14, 255}
	get := func(x, y int) (color.NRGBA, bool) {
		if y >= c.h || c.p[y][x].a == 0 {
			return color.NRGBA{}, false
		}
		p := c.p[y][x]
		if p.a < 1 {
			return lerp(dark, p.c, p.a), true
		}
		return p.c, true
	}
	var b strings.Builder
	for y := 0; y < c.h; y += 2 {
		line := strings.Builder{}
		for x := range c.w {
			top, okT := get(x, y)
			bot, okB := get(x, y+1)
			switch {
			case okT && okB && top == bot:
				fmt.Fprintf(&line, "\x1b[38;2;%d;%d;%dm█", top.R, top.G, top.B)
			case okT && okB:
				fmt.Fprintf(&line, "\x1b[38;2;%d;%d;%d;48;2;%d;%d;%dm▀\x1b[49m", top.R, top.G, top.B, bot.R, bot.G, bot.B)
			case okT:
				fmt.Fprintf(&line, "\x1b[38;2;%d;%d;%dm▀", top.R, top.G, top.B)
			case okB:
				fmt.Fprintf(&line, "\x1b[38;2;%d;%d;%dm▄", bot.R, bot.G, bot.B)
			default:
				line.WriteByte(' ')
			}
		}
		b.WriteString(strings.TrimRight(line.String(), " "))
		b.WriteString("\x1b[0m\n")
	}
	return b.String()
}

// pad returns c centred on a larger transparent canvas.
func pad(c *canvas, n int) *canvas {
	out := newCanvas(c.w+2*n, c.h+2*n)
	blit(out, c, n, n)
	return out
}

func main() {
	preview := flag.Bool("preview", false, "print the logos to the terminal instead of writing files")
	name := flag.String("theme", "ember", "color theme")
	shots := flag.String("shots", "", "write a preview PNG of every theme into this directory")
	flag.Parse()
	if *shots != "" {
		bg := hex("#0d1117")
		for n, t := range themes {
			th = t
			l := lockup()
			if err := l.png(filepath.Join(*shots, n+".png"), 10, &bg); err != nil {
				fmt.Fprintln(os.Stderr, "gen:", err)
				os.Exit(1)
			}
		}
		return
	}
	t, ok := themes[*name]
	if !ok {
		fmt.Fprintln(os.Stderr, "gen: unknown theme", *name)
		os.Exit(2)
	}
	th = t

	logo := wordmark([]int{9, 15, 6, 12})
	icon := portal(true, true)
	if *preview {
		fmt.Printf("\n%s\n%s\n%s\n", logo.ansi(), icon.ansi(), lockup().ansi())
		return
	}

	dir := "assets"
	bg := hex("#0d1117")
	steps := []func() error{
		func() error { return logo.svg(filepath.Join(dir, "logo.svg")) },
		func() error { return logo.png(filepath.Join(dir, "logo.png"), 16, nil) },
		func() error { return icon.svg(filepath.Join(dir, "icon.svg")) },
		func() error { return pad(icon, 2).png(filepath.Join(dir, "icon.png"), 20, &bg) },
		func() error { return os.WriteFile(filepath.Join(dir, "logo.ans"), []byte(logo.ansi()), 0o644) },
		func() error { return os.WriteFile("cmd/rift/logo.ans", []byte(logo.ansi()), 0o644) },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			fmt.Fprintln(os.Stderr, "gen:", err)
			os.Exit(1)
		}
	}
}
