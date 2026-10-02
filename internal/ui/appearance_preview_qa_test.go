package ui

import (
	"context"
	"fmt"
	"github.com/rbeene/tempo/internal/terminal"
	"strings"
	"testing"
	"time"
)

type qaPromptCandidateStyle string

func (s qaPromptCandidateStyle) Paint(role terminal.Role, text string) string {
	return fmt.Sprintf("<%s:%s>%s", s, role, text)
}

func TestQAPromptCandidateStyles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newPromptBridge(ctx)
	styles := map[string]terminal.Styler{"a": qaPromptCandidateStyle("A"), "b": qaPromptCandidateStyle("B")}
	choices := []terminal.Choice{{ID: "a", Label: "Alpha"}, {ID: "b", Label: "Beta"}}
	done := make(chan error, 1)
	go func() {
		id, err := p.chooseStyled(ctx, "Preview", choices, styles)
		if err == nil && id != "b" {
			err = fmt.Errorf("choice=%s, wantb", id)
		}
		done <- err
	}()
	var r promptRequest
	select {
	case r = <-p.requests:
	case err := <-done:
		t.Fatalf("styled broker skipped modal: %v", err)
	case <-time.After(time.Second):
		t.Fatal("styled broker stalled")
	}
	styles["a"], styles["b"] = qaPromptCandidateStyle("FOREIGN"), qaPromptCandidateStyle("FOREIGN")
	choices[1] = terminal.Choice{ID: "foreign", Label: "Foreign"}
	m := newPromptModel(r, 80, 24)
	defer m.Close()
	r.styles["b"] = qaPromptCandidateStyle("REQUEST_MUTATED")
	for _, tc := range []struct {
		marker string
		event  terminal.Event
	}{{"A", terminal.Event{}}, {"B", terminal.Event{Kind: "down"}}} {
		if tc.event.Kind != "" {
			qaPromptPending(t, m, tc.event)
		}
		frame := strings.Join(m.Render(qaPromptCandidateStyle("BASE")), "\n")
		if !strings.Contains(frame, "<"+tc.marker+":accent>Preview") || strings.Contains(frame, "FOREIGN") || strings.Contains(frame, "REQUEST_MUTATED") || strings.Contains(frame, "BASE") {
			t.Errorf("request-local immutable style resolver failed: %s", frame)
		}
	}
	r.reply <- qaPromptSubmit(t, m)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("styled choice did not finish")
	}
	plain := newPromptModel(promptRequest{kind: "choose", ctx: ctx, title: "Plain", choices: []terminal.Choice{{ID: "x", Label: "Plain"}}}, 80, 24)
	defer plain.Close()
	if got := strings.Join(plain.Render(qaPromptCandidateStyle("BASE")), "\n"); !strings.Contains(got, "<BASE:accent>Plain") || strings.Contains(got, "<A:") || strings.Contains(got, "<B:") {
		t.Errorf("candidate styling leaked across requests: %s", got)
	}
	for _, kind := range []string{"confirm", "view"} {
		go func() {
			if kind == "confirm" {
				_, err := p.confirmStyled(ctx, "Scoped", qaPromptCandidateStyle("OVERRIDE"))
				done <- err
			} else {
				done <- p.viewStyled(ctx, "Details", "Scoped body", qaPromptCandidateStyle("OVERRIDE"))
			}
		}()
		select {
		case r = <-p.requests:
		case err := <-done:
			t.Fatalf("styled %s skipped modal: %v", kind, err)
		case <-time.After(time.Second):
			t.Fatal("styled modal stalled")
		}
		model := newPromptModel(r, 80, 24)
		if got := strings.Join(model.Render(qaPromptCandidateStyle("BASE")), "\n"); !strings.Contains(got, "<OVERRIDE:") || strings.Contains(got, "<BASE:") {
			t.Errorf("%s override not applied to entire request: %s", kind, got)
		}
		r.reply <- promptReply{}
		model.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("styled modal did not finish")
		}
	}
}
