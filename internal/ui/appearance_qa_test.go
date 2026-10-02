package ui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
)

var qaAppearanceCaps = themes.Capabilities{OutputTTY: true, Term: "xterm-256color", ColorTerm: "truecolor"}

func qaAppearancePath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "preferences.json")
}
func qaAppearanceService(path string, fault func(string) error) *themes.Service {
	return themes.New(themes.Options{Path: path, Fault: fault, Getenv: func(string) string { panic("Appearance explicit path queried environment") }})
}
func qaAppearanceSave(t *testing.T, path, theme, revision string, n int) {
	t.Helper()
	_, err := qaAppearanceService(path, nil).Set(context.Background(), themes.SetInput{Theme: theme, IfRevision: revision, RequestID: fmt.Sprintf("88888888-8888-4888-8888-%012d", n)})
	if err != nil {
		t.Fatal(err)
	}
}
func qaAppearanceSaved(t *testing.T, path, theme, revision string) {
	t.Helper()
	got, err := qaAppearanceService(path, nil).Show(context.Background(), "")
	if err != nil || got.Theme.ID != theme || got.PreferenceRevision != revision || !got.Theme.Selected {
		t.Fatalf("saved preference %+v err=%v; want %s/%s", got, err, theme, revision)
	}
}
func qaAppearanceBytes(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type qaAppearanceResult struct {
	style terminal.Styler
	err   error
}
type qaAppearanceFlow struct {
	bridge *promptBridge
	done   chan qaAppearanceResult
	cancel context.CancelCauseFunc
}

func qaAppearanceStart(t *testing.T, s *themes.Service) *qaAppearanceFlow {
	return qaAppearanceStartRun(t, func(ctx context.Context, bridge *promptBridge) (terminal.Styler, error) {
		return showAppearance(ctx, bridge, s, qaAppearanceCaps)
	})
}
func qaAppearanceStartRun(t *testing.T, run func(context.Context, *promptBridge) (terminal.Styler, error)) *qaAppearanceFlow {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	x := &qaAppearanceFlow{bridge: newPromptBridge(ctx), done: make(chan qaAppearanceResult, 1), cancel: cancel}
	go func() {
		style, err := run(ctx, x.bridge)
		x.done <- qaAppearanceResult{style, err}
	}()
	t.Cleanup(func() {
		cancel(context.Canceled)
		select {
		case <-x.done:
		case <-time.After(time.Second):
			t.Error("Appearance flow survived cancellation")
		}
	})
	return x
}

func TestQAAppearanceReopenRetainsPendingIdentityUntilConclusiveReplay(t *testing.T) {
	path := qaAppearancePath(t)
	qaAppearanceSave(t, path, "tokyo-night", "0", 1)
	var syncs atomic.Int32
	f := newAppearanceFlow(qaAppearanceService(path, func(stage string) error {
		if stage == "directory_sync" && syncs.Add(1) == 1 {
			return errors.New("PRIVATE durability body")
		}
		return nil
	}), qaAppearanceCaps)
	x := qaAppearanceStartRun(t, f.run)
	qaAppearanceChoose(t, x.request(t, "choose"), "gruvbox")
	x.request(t, "confirm").reply <- promptReply{confirmed: true}
	r := x.request(t, "view")
	r.reply <- promptReply{err: &terminal.ExitError{Code: 0}}
	result := x.finish(t)
	var first *themes.Error
	if !errors.As(result.err, &first) || !first.Uncertain {
		t.Fatalf("first unknown lost: %v", result.err)
	}
	id, ok := first.Details["request_id"].(string)
	if !ok || id == "" {
		t.Fatal("unknown has no replay identity")
	}
	qaAppearanceSave(t, path, "catppuccin", "2", 2)
	external := qaAppearanceBytes(t, path)
	for _, retry := range []bool{false, true} {
		x = qaAppearanceStartRun(t, f.run)
		r = x.request(t, "view")
		if !strings.Contains(r.body, id) || !strings.Contains(r.body, "gruvbox") || !strings.Contains(r.body, "local_write_unknown") {
			t.Errorf("reopen discarded original unknown intent: %s", r.body)
		}
		r.reply <- promptReply{}
		r = x.request(t, "confirm")
		if !strings.Contains(r.title, id) {
			t.Error("reopen replaced original request ID")
		}
		r.reply <- promptReply{confirmed: retry}
		result = x.finish(t)
		if retry {
			qaAppearanceNoError(t, result.err)
			qaAppearanceCurrentStyle(t, result.style, "catppuccin")
		} else {
			var safe *themes.Error
			if !errors.As(result.err, &safe) || safe.Details["request_id"] != id || !safe.Uncertain {
				t.Fatalf("declined reopened retry discarded pending identity: %v", result.err)
			}
		}
		if !bytes.Equal(external, qaAppearanceBytes(t, path)) {
			t.Fatal("reopened replay rolled back later writer or stored another receipt")
		}
	}
	if syncs.Load() != 2 {
		t.Errorf("same-ID conclusive replay durability barriers=%d want2", syncs.Load())
	}
	x = qaAppearanceStartRun(t, f.run)
	r = x.request(t, "choose")
	r.reply <- promptReply{err: &terminal.ExitError{Code: 0}}
	result = x.finish(t)
	qaAppearanceNoError(t, result.err)
	qaAppearanceCurrentStyle(t, result.style, "catppuccin")
}
func (x *qaAppearanceFlow) request(t *testing.T, kind string) promptRequest {
	t.Helper()
	select {
	case r := <-x.bridge.requests:
		if r.kind != kind {
			t.Fatalf("prompt kind %s, want %s", r.kind, kind)
		}
		return r
	case result := <-x.done:
		x.done <- result
		t.Fatalf("Appearance returned before %s prompt: %v", kind, result.err)
	case <-time.After(time.Second):
		t.Fatalf("Appearance did not present %s prompt", kind)
	}
	return promptRequest{}
}
func (x *qaAppearanceFlow) finish(t *testing.T) qaAppearanceResult {
	t.Helper()
	select {
	case result := <-x.done:
		x.done <- result
		return result
	case <-time.After(time.Second):
		t.Fatal("Appearance flow failed to join")
	}
	return qaAppearanceResult{}
}
func qaAppearanceCurrentStyle(t *testing.T, style terminal.Styler, id string) {
	t.Helper()
	want, err := themes.NewStyler(id, qaAppearanceCaps)
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []terminal.Role{terminal.RoleText, terminal.RoleAccent, terminal.RoleSelection, terminal.RoleKey} {
		if style == nil || style.Paint(role, "Visible status") != want.Paint(role, "Visible status") {
			t.Errorf("returned style is not current persisted %s, role %s", id, role)
		}
	}
}
func qaAppearanceNoError(t *testing.T, err error) {
	t.Helper()
	var exit *terminal.ExitError
	if err != nil && !(errors.As(err, &exit) && exit.Code == 0) {
		t.Fatalf("Appearance outcome %v", err)
	}
}
func qaAppearanceCandidate(t *testing.T, request promptRequest, id string) *promptModel {
	t.Helper()
	m := newPromptModel(request, 120, 40)
	t.Cleanup(m.Close)
	if len(m.choices) != 4 {
		t.Fatalf("Appearance choices=%d, want all four themes", len(m.choices))
	}
	qaPromptPending(t, m, terminal.Event{Kind: "text", Text: id})
	frame := strings.Join(m.Render(nil), "\n")
	want, err := themes.NewStyler(id, qaAppearanceCaps)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(frame, want.Paint(terminal.RoleAccent, m.title)) {
		t.Errorf("candidate %s preview title lacks candidate style: %q", id, frame)
	}
	return m
}
func qaAppearanceChoose(t *testing.T, request promptRequest, id string) {
	t.Helper()
	m := qaAppearanceCandidate(t, request, id)
	request.reply <- qaPromptSubmit(t, m)
}

func TestQAAppearancePreviewCancelRereadsExternalWriterWithoutRollback(t *testing.T) {
	for _, cancelAt := range []string{"picker", "confirmation"} {
		t.Run(cancelAt, func(t *testing.T) {
			path := qaAppearancePath(t)
			qaAppearanceSave(t, path, "tokyo-night", "0", 1)
			before := qaAppearanceBytes(t, path)
			x := qaAppearanceStart(t, qaAppearanceService(path, nil))
			r := x.request(t, "choose")
			m := newPromptModel(r, 120, 40)
			t.Cleanup(m.Close)
			if got := m.matches()[m.selected].ID; got != "tokyo-night" {
				t.Errorf("initial candidate %s is not saved Tokyo Night", got)
			}
			qaPromptPending(t, m, terminal.Event{Kind: "down"})
			candidate := m.matches()[m.selected].ID
			want, _ := themes.NewStyler(candidate, qaAppearanceCaps)
			if !strings.Contains(strings.Join(m.Render(nil), "\n"), want.Paint(terminal.RoleAccent, m.title)) {
				t.Error("arrow preview did not immediately use candidate")
			}
			if !bytes.Equal(before, qaAppearanceBytes(t, path)) {
				t.Fatal("preview persisted preference")
			}
			if cancelAt == "confirmation" {
				qaAppearanceChoose(t, r, "gruvbox")
				r = x.request(t, "confirm")
			}
			qaAppearanceSave(t, path, "catppuccin", "1", 2)
			external := qaAppearanceBytes(t, path)
			if cancelAt == "picker" {
				r.reply <- promptReply{err: &terminal.ExitError{Code: 0}}
			} else {
				r.reply <- promptReply{confirmed: false}
			}
			result := x.finish(t)
			qaAppearanceNoError(t, result.err)
			qaAppearanceCurrentStyle(t, result.style, "catppuccin")
			if !bytes.Equal(external, qaAppearanceBytes(t, path)) {
				t.Fatal("cancel rolled back or wrote external preference")
			}
		})
	}
}

func TestQAAppearanceApplyAndResetUseSharedPreferenceCAS(t *testing.T) {
	for _, id := range []string{"tokyo-night", "gruvbox", "catppuccin", "terminal-default"} {
		t.Run(id, func(t *testing.T) {
			path := qaAppearancePath(t)
			initial := "catppuccin"
			if id == initial || id == "terminal-default" {
				initial = "tokyo-night"
			}
			qaAppearanceSave(t, path, initial, "0", 1)
			x := qaAppearanceStart(t, qaAppearanceService(path, nil))
			qaAppearanceChoose(t, x.request(t, "choose"), id)
			r := x.request(t, "confirm")
			m := newPromptModel(r, 120, 40)
			t.Cleanup(m.Close)
			want, _ := themes.NewStyler(id, qaAppearanceCaps)
			if !strings.Contains(strings.Join(m.Render(nil), "\n"), want.Paint(terminal.RoleAccent, "Review scoped change")) {
				t.Error("confirmation lost candidate palette")
			}
			qaPromptPending(t, m, terminal.Event{Kind: "text", Text: "y"})
			r.reply <- qaPromptSubmit(t, m)
			result := x.finish(t)
			qaAppearanceNoError(t, result.err)
			qaAppearanceSaved(t, path, id, "2")
			qaAppearanceCurrentStyle(t, result.style, id)
			var state struct {
				Requests map[string]struct {
					Intent struct {
						Theme      string `json:"theme"`
						IfRevision string `json:"if_revision"`
					}
					Result struct {
						RequestID string `json:"request_id"`
					}
				}
			}
			if err := json.Unmarshal(qaAppearanceBytes(t, path), &state); err != nil {
				t.Fatal(err)
			}
			if len(state.Requests) != 2 {
				t.Fatalf("Apply request receipts=%d want2", len(state.Requests))
			}
			for requestID, receipt := range state.Requests {
				if requestID == "88888888-8888-4888-8888-000000000001" {
					continue
				}
				if len(requestID) != 36 || receipt.Result.RequestID != requestID || receipt.Intent.Theme != id || receipt.Intent.IfRevision != "1" {
					t.Errorf("Apply lacks one stable UUID and observed CAS intent: %+v", receipt)
				}
			}
		})
	}
}

type qaAppearanceMarkerStyle string

func (s qaAppearanceMarkerStyle) Paint(role terminal.Role, text string) string {
	return fmt.Sprintf("<%s:%s>%s", s, role, text)
}

func TestQAAppearanceBrokerStylesAreCopiedAndRequestLocal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newPromptBridge(ctx)
	styles := map[string]terminal.Styler{"a": qaAppearanceMarkerStyle("A"), "b": qaAppearanceMarkerStyle("B")}
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
	styles["a"], styles["b"] = qaAppearanceMarkerStyle("FOREIGN"), qaAppearanceMarkerStyle("FOREIGN")
	choices[1] = terminal.Choice{ID: "foreign", Label: "Foreign"}
	m := newPromptModel(r, 80, 24)
	defer m.Close()
	r.styles["b"] = qaAppearanceMarkerStyle("REQUEST_MUTATED")
	for _, tc := range []struct {
		marker string
		event  terminal.Event
	}{{"A", terminal.Event{}}, {"B", terminal.Event{Kind: "down"}}} {
		if tc.event.Kind != "" {
			qaPromptPending(t, m, tc.event)
		}
		frame := strings.Join(m.Render(qaAppearanceMarkerStyle("BASE")), "\n")
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
	if got := strings.Join(plain.Render(qaAppearanceMarkerStyle("BASE")), "\n"); !strings.Contains(got, "<BASE:accent>Plain") || strings.Contains(got, "<A:") || strings.Contains(got, "<B:") {
		t.Errorf("candidate styling leaked across requests: %s", got)
	}
	for _, kind := range []string{"confirm", "view"} {
		go func() {
			if kind == "confirm" {
				_, err := p.confirmStyled(ctx, "Scoped", qaAppearanceMarkerStyle("OVERRIDE"))
				done <- err
			} else {
				done <- p.viewStyled(ctx, "Details", "Scoped body", qaAppearanceMarkerStyle("OVERRIDE"))
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
		if got := strings.Join(model.Render(qaAppearanceMarkerStyle("BASE")), "\n"); !strings.Contains(got, "<OVERRIDE:") || strings.Contains(got, "<BASE:") {
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

func TestQAAppearanceStaleDraftRequiresFreshCandidateAndConfirmation(t *testing.T) {
	path := qaAppearancePath(t)
	qaAppearanceSave(t, path, "tokyo-night", "0", 1)
	x := qaAppearanceStart(t, qaAppearanceService(path, nil))
	qaAppearanceChoose(t, x.request(t, "choose"), "gruvbox")
	r := x.request(t, "confirm")
	qaAppearanceSave(t, path, "catppuccin", "1", 2)
	external := qaAppearanceBytes(t, path)
	r.reply <- promptReply{confirmed: true}
	outcome := x.request(t, "view")
	if !strings.Contains(outcome.body, "revision_conflict") {
		t.Errorf("stale Apply lacks safe conflict outcome: %s", outcome.body)
	}
	if !bytes.Equal(external, qaAppearanceBytes(t, path)) {
		t.Fatal("stale draft silently rebased or wrote")
	}
	outcome.reply <- promptReply{}
	r = x.request(t, "choose")
	qaAppearanceChoose(t, r, "gruvbox")
	r = x.request(t, "confirm")
	if !bytes.Equal(external, qaAppearanceBytes(t, path)) {
		t.Fatal("reload applied without fresh confirmation")
	}
	r.reply <- promptReply{confirmed: true}
	result := x.finish(t)
	qaAppearanceNoError(t, result.err)
	qaAppearanceSaved(t, path, "gruvbox", "3")
	qaAppearanceCurrentStyle(t, result.style, "gruvbox")
}

func TestQAAppearanceUnknownRetryKeepsIdentityAndRefreshesHistoricalReplay(t *testing.T) {
	path := qaAppearancePath(t)
	qaAppearanceSave(t, path, "tokyo-night", "0", 1)
	var directorySyncs atomic.Int32
	x := qaAppearanceStart(t, qaAppearanceService(path, func(stage string) error {
		if stage == "directory_sync" && directorySyncs.Add(1) == 1 {
			return errors.New("PRIVATE injected durability body")
		}
		return nil
	}))
	qaAppearanceChoose(t, x.request(t, "choose"), "gruvbox")
	x.request(t, "confirm").reply <- promptReply{confirmed: true}
	r := x.request(t, "view")
	if !strings.Contains(r.body, "local_write_unknown") || strings.Contains(r.body, "PRIVATE") {
		t.Errorf("unsafe/missing uncertainty outcome: %s", r.body)
	}
	qaAppearanceSaved(t, path, "gruvbox", "2")
	uncertain := qaAppearanceBytes(t, path)
	var st struct{ Requests map[string]json.RawMessage }
	if err := json.Unmarshal(uncertain, &st); err != nil {
		t.Fatal(err)
	}
	var requestID string
	for id := range st.Requests {
		if id != "88888888-8888-4888-8888-000000000001" {
			requestID = id
		}
	}
	if requestID == "" || !strings.Contains(r.body, requestID) {
		t.Fatal("unknown outcome did not retain exact request ID")
	}
	qaAppearanceSave(t, path, "catppuccin", "2", 2)
	external := qaAppearanceBytes(t, path)
	r.reply <- promptReply{}
	r = x.request(t, "confirm")
	if !strings.Contains(r.title, requestID) {
		t.Error("same-ID retry confirmation does not identify retained request")
	}
	r.reply <- promptReply{confirmed: true}
	result := x.finish(t)
	qaAppearanceNoError(t, result.err)
	qaAppearanceCurrentStyle(t, result.style, "catppuccin")
	qaAppearanceSaved(t, path, "catppuccin", "3")
	if !bytes.Equal(external, qaAppearanceBytes(t, path)) || directorySyncs.Load() != 2 {
		t.Error("historical retry rewrote selection, added receipt, or skipped durability barrier")
	}
}

func TestQAAppearanceUnknownSurvivesCancelledOutcomeOrRetry(t *testing.T) {
	for _, mode := range []string{"outcome-escape", "retry-decline", "context-signal"} {
		t.Run(mode, func(t *testing.T) {
			path := qaAppearancePath(t)
			qaAppearanceSave(t, path, "tokyo-night", "0", 1)
			x := qaAppearanceStart(t, qaAppearanceService(path, func(stage string) error {
				if stage == "directory_sync" {
					return errors.New("PRIVATE durability")
				}
				return nil
			}))
			qaAppearanceChoose(t, x.request(t, "choose"), "gruvbox")
			x.request(t, "confirm").reply <- promptReply{confirmed: true}
			r := x.request(t, "view")
			if mode == "retry-decline" {
				r.reply <- promptReply{}
				x.request(t, "confirm").reply <- promptReply{confirmed: false}
			} else if mode == "context-signal" {
				x.cancel(&terminal.ExitError{Code: 143})
			} else {
				r.reply <- promptReply{err: &terminal.ExitError{Code: 0}}
			}
			result := x.finish(t)
			var safe *themes.Error
			if !errors.As(result.err, &safe) || safe.Code != "local_write_unknown" || !safe.Uncertain || safe.Retryable {
				t.Fatalf("unknown commit discarded by %s: %v", mode, result.err)
			}
			if safe.Details["request_id"] == "" || safe.Details["theme"] != "gruvbox" || safe.Details["if_revision"] != "1" {
				t.Errorf("unknown exit lacks exact safe replay inputs: %+v", safe.Details)
			}
			qaAppearanceSaved(t, path, "gruvbox", "2")
		})
	}
}

func TestQAAppearanceCorruptPreferencesArePreservedAndActionable(t *testing.T) {
	path := qaAppearancePath(t)
	before := []byte("{PRIVATE broken synthetic preferences")
	if err := os.WriteFile(path, before, 0600); err != nil {
		t.Fatal(err)
	}
	x := qaAppearanceStart(t, qaAppearanceService(path, nil))
	result := x.finish(t)
	var safe *themes.Error
	if !errors.As(result.err, &safe) || safe.Code != "state_corrupt" || strings.Contains(result.err.Error(), "PRIVATE") {
		t.Errorf("corrupt preferences not safely actionable: %v", result.err)
	}
	if !bytes.Equal(before, qaAppearanceBytes(t, path)) {
		t.Error("Appearance reset corrupt bytes")
	}
	select {
	case <-x.bridge.requests:
		t.Error("corrupt preferences offered blind apply")
	default:
	}
}
