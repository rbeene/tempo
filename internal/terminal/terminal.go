// Package terminal owns interactive input and terminal restoration.
package terminal

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/term"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

type Choice struct{ ID, Label string }
type Prompter interface {
	Choose(context.Context, string, []Choice) (string, error)
	Text(context.Context, string, string) (string, error)
	Secret(context.Context, string) ([]byte, error)
	Confirm(context.Context, string) (bool, error)
}
type ExitError struct{ Code int }

func (e *ExitError) Error() string { return "terminal session ended" }

type Session struct {
	ctx                         context.Context
	cancel                      context.CancelCauseFunc
	keys                        chan string
	stopped                     chan struct{}
	in, out                     *os.File
	original                    *os.File
	state                       *term.State
	restoreInput, restoreOutput func()
	once                        sync.Once
	closeErr                    error
	lines                       int
}

func Eligible(in io.Reader, out io.Writer) bool {
	i, ok := in.(*os.File)
	o, ok2 := out.(*os.File)
	return ok && ok2 && term.IsTerminal(int(i.Fd())) && term.IsTerminal(int(o.Fd()))
}
func Open(ctx context.Context, in io.Reader, out io.Writer) (*Session, error) {
	if !Eligible(in, out) {
		return nil, &ExitError{1}
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	original := in.(*os.File)
	state, e := term.MakeRaw(int(original.Fd()))
	if e != nil {
		return nil, &ExitError{1}
	}
	s := &Session{ctx: ctx, original: original, state: state}
	s.in, s.restoreInput, e = duplicateTerminal(original)
	if e != nil {
		term.Restore(int(original.Fd()), state)
		return nil, &ExitError{1}
	}
	s.out, s.restoreOutput, e = duplicateTerminal(out.(*os.File))
	if e != nil {
		s.restoreInput()
		term.Restore(int(original.Fd()), state)
		return nil, &ExitError{1}
	}
	s.ctx, s.cancel = context.WithCancelCause(ctx)
	s.keys = make(chan string, 32768)
	s.stopped = make(chan struct{})
	go s.pump()
	return s, nil
}
func (s *Session) Close() error {
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel(&ExitError{Code: 0})
			<-s.stopped
		}
		if s.restoreOutput != nil {
			s.restoreOutput()
		}
		if s.restoreInput != nil {
			s.restoreInput()
		}
		if s.state != nil {
			s.closeErr = term.Restore(int(s.original.Fd()), s.state)
		}
	})
	return s.closeErr
}
func Sanitize(v string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == unicode.ReplacementChar {
			return -1
		}
		return r
	}, v)
}
func (s *Session) write(ctx context.Context, text string) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if s.ctx.Err() != nil {
		return context.Cause(s.ctx)
	}
	s.out.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
	if _, e := io.WriteString(s.out, text); e != nil {
		return &ExitError{1}
	}
	return nil
}
func (s *Session) byte(ctx context.Context, until time.Time) (byte, error) {
	for {
		if ctx.Err() != nil {
			return 0, context.Cause(ctx)
		}
		if s.ctx.Err() != nil {
			return 0, context.Cause(s.ctx)
		}
		if !until.IsZero() && time.Now().After(until) {
			return 0, os.ErrDeadlineExceeded
		}
		deadline := time.Now().Add(25 * time.Millisecond)
		if !until.IsZero() && until.Before(deadline) {
			deadline = until
		}
		s.in.SetReadDeadline(deadline)
		var b [1]byte
		n, e := s.in.Read(b[:])
		if n > 0 {
			return b[0], nil
		}
		if errors.Is(e, os.ErrDeadlineExceeded) {
			continue
		}
		if errors.Is(e, io.EOF) {
			return 0, &ExitError{0}
		}
		if e != nil {
			return 0, &ExitError{1}
		}
	}
}
func (s *Session) readKey(ctx context.Context) (string, error) {
	b, e := s.byte(ctx, time.Time{})
	if e != nil {
		return "", e
	}
	switch b {
	case 3:
		return "", &ExitError{130}
	case 4:
		return "", &ExitError{0}
	case 13, 10:
		return "enter", nil
	case 127, 8:
		return "backspace", nil
	case 27:
		b, e = s.byte(ctx, time.Now().Add(40*time.Millisecond))
		if errors.Is(e, os.ErrDeadlineExceeded) {
			return "escape", nil
		}
		if e != nil {
			return "", e
		}
		if b != '[' {
			return "", &ExitError{0}
		}
		b, e = s.byte(ctx, time.Now().Add(40*time.Millisecond))
		if e != nil {
			return "", &ExitError{0}
		}
		if b == 'A' {
			return "up", nil
		}
		if b == 'B' {
			return "down", nil
		}
		return "", nil
	}
	if b < 32 {
		return "", nil
	}
	data := []byte{b}
	for !utf8.FullRune(data) && len(data) < 4 {
		b, e = s.byte(ctx, time.Time{})
		if e != nil {
			return "", e
		}
		data = append(data, b)
	}
	r, _ := utf8.DecodeRune(data)
	if r == utf8.RuneError || unicode.IsControl(r) {
		return "", nil
	}
	return "text:" + string(data), nil
}
func (s *Session) render(ctx context.Context, title string, lines []string) error {
	var b strings.Builder
	if s.lines > 0 {
		fmt.Fprintf(&b, "\x1b[%dA\r\x1b[J", s.lines)
	}
	b.WriteString(Sanitize(title) + "\r\n")
	for _, line := range lines {
		b.WriteString(line + "\r\n")
	}
	s.lines = len(lines) + 1
	return s.write(ctx, b.String())
}
func (s *Session) Choose(ctx context.Context, title string, choices []Choice) (string, error) {
	query := ""
	selected := 0
	defer func() { s.lines = 0 }()
	for {
		filtered := []Choice{}
		for _, c := range choices {
			if strings.Contains(strings.ToLower(c.Label+" "+c.ID), strings.ToLower(query)) {
				filtered = append(filtered, c)
			}
		}
		if selected >= len(filtered) {
			selected = 0
		}
		lines := []string{"Search: " + query + "  (↑/↓ Enter select; Escape cancel)"}
		if len(filtered) == 0 {
			lines = append(lines, "No matches")
		}
		start := 0
		if selected > 7 {
			start = selected - 7
		}
		end := start + 8
		if end > len(filtered) {
			end = len(filtered)
		}
		for i := start; i < end; i++ {
			prefix := "  "
			if i == selected {
				prefix = "> "
			}
			lines = append(lines, prefix+Sanitize(filtered[i].Label))
		}
		if e := s.render(ctx, title, lines); e != nil {
			return "", e
		}
		k, e := s.key(ctx)
		if e != nil {
			return "", e
		}
		switch k {
		case "enter":
			if len(filtered) > 0 {
				return filtered[selected].ID, nil
			}
		case "up":
			if selected > 0 {
				selected--
			}
		case "down":
			if selected+1 < len(filtered) {
				selected++
			}
		case "backspace":
			r := []rune(query)
			if len(r) > 0 {
				query = string(r[:len(r)-1])
				selected = 0
			}
		default:
			if strings.HasPrefix(k, "text:") && len(query) < 256 {
				query += strings.TrimPrefix(k, "text:")
				selected = 0
			}
		}
	}
}
func (s *Session) input(ctx context.Context, title, defaultValue string, secret bool) ([]byte, error) {
	defer func() { s.lines = 0 }()
	data := []byte{}
	defer func() { clear(data) }()
	prompt := Sanitize(title)
	if defaultValue != "" && !secret {
		prompt += " [" + Sanitize(defaultValue) + "]"
	}
	if e := s.write(ctx, prompt+": "); e != nil {
		return nil, e
	}
	for {
		k, e := s.key(ctx)
		if e != nil {
			return nil, e
		}
		switch k {
		case "enter":
			if e = s.write(ctx, "\r\n"); e != nil {
				return nil, e
			}
			if len(data) == 0 && !secret {
				return []byte(defaultValue), nil
			}
			return append([]byte(nil), data...), nil
		case "backspace":
			if len(data) > 0 {
				_, n := utf8.DecodeLastRune(data)
				data = data[:len(data)-n]
				if !secret {
					if e = s.write(ctx, "\b \b"); e != nil {
						return nil, e
					}
				}
			}
		default:
			if strings.HasPrefix(k, "text:") {
				v := strings.TrimPrefix(k, "text:")
				if len(data)+len(v) > 16384 {
					return nil, &ExitError{1}
				}
				data = append(data, v...)
				if !secret {
					if e = s.write(ctx, v); e != nil {
						return nil, e
					}
				}
			}
		}
	}
}
func (s *Session) Text(c context.Context, t, d string) (string, error) {
	v, e := s.input(c, t, d, false)
	return string(v), e
}
func (s *Session) Secret(c context.Context, t string) ([]byte, error) { return s.input(c, t, "", true) }
func (s *Session) Confirm(c context.Context, t string) (bool, error) {
	id, e := s.Choose(c, t, []Choice{{"no", "Cancel"}, {"yes", "Confirm"}})
	return id == "yes", e
}

// Context is the terminal owner's action context. It ends on external
// cancellation or terminal interruption and must be used for interactive actions.
func (s *Session) Context() context.Context { return s.ctx }

func (s *Session) pump() {
	defer close(s.stopped)
	for {
		key, e := s.readKey(s.ctx)
		if e != nil {
			s.cancel(e)
			return
		}
		select {
		case s.keys <- key:
		case <-s.ctx.Done():
			return
		default:
			s.cancel(&ExitError{Code: 1})
			return
		}
	}
}
func (s *Session) key(ctx context.Context) (string, error) {
	if s.ctx.Err() != nil {
		return "", context.Cause(s.ctx)
	}
	select {
	case <-ctx.Done():
		return "", context.Cause(ctx)
	case <-s.ctx.Done():
		return "", context.Cause(s.ctx)
	case k := <-s.keys:
		if k == "escape" {
			return "", &ExitError{Code: 0}
		}
		return k, nil
	}
}
