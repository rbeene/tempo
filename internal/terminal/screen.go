package terminal

import (
	"context"
	"golang.org/x/term"
	"strings"
)

// Event is input for the single presentation consumer. Text is populated only
// for text or paste events; resize events carry terminal dimensions.
type Event struct {
	Kind, Text    string
	Columns, Rows int
}

// Next consumes one event without giving dashboard Escape quit semantics.
// The caller is the sole input consumer, including while displaying prompts.
func (s *Session) Next(ctx context.Context) (Event, error) {
	if s.ctx.Err() != nil {
		return Event{}, context.Cause(s.ctx)
	}
	if ctx.Err() != nil {
		return Event{}, context.Cause(ctx)
	}
	select {
	case <-ctx.Done():
		return Event{}, context.Cause(ctx)
	case <-s.ctx.Done():
		return Event{}, context.Cause(s.ctx)
	case <-s.resize:
		// Observe dimensions at consumption, not at signal arrival, and collapse
		// queued notifications into the latest terminal size.
		for {
			select {
			case <-s.resize:
				continue
			default:
				columns, rows, err := s.Size()
				return Event{Kind: "resize", Columns: columns, Rows: rows}, err
			}
		}
	case k := <-s.keys:
		for _, kind := range []string{"text", "paste"} {
			if text, ok := strings.CutPrefix(k, kind+":"); ok {
				return Event{Kind: kind, Text: text}, nil
			}
		}
		return Event{Kind: k}, nil
	}
}

func (s *Session) Size() (int, int, error) { return term.GetSize(int(s.out.Fd())) }

// EnterScreen acquires terminal presentation modes. Mark acquisition before
// writing so Close also unwinds a partially written entry sequence.
func (s *Session) EnterScreen(ctx context.Context) error {
	if s.screen {
		return nil
	}
	s.screen = true
	if err := s.write(ctx, "\x1b[?1049h\x1b[?25l"); err != nil {
		return err
	}
	return s.ensurePaste(ctx)
}

// Draw accepts only trusted, sanitized and width-bounded renderer lines.
func (s *Session) Draw(ctx context.Context, lines []string) error {
	return s.write(ctx, "\x1b[H\x1b[J"+strings.Join(lines, "\r\n"))
}

// ensurePaste is shared by full-screen and finite prompts. It is acquired before
// prompting and restored once by Close, including after a partial write.
func (s *Session) ensurePaste(ctx context.Context) error {
	if s.paste {
		return nil
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if s.ctx.Err() != nil {
		return context.Cause(s.ctx)
	}
	// A closed output cannot acquire a mode. Mark ownership only once the
	// bounded writer is ready to dispatch, retaining partial-write cleanup.
	return s.writeOutputAcquiring(ctx, "\x1b[?2004h", &s.paste)
}
