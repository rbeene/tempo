package ui

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rivo/uniseg"
)

func qaPromptRequest(kind, title string) promptRequest {
	return promptRequest{kind: kind, title: title, ctx: context.Background(), reply: make(chan promptReply, 1)}
}
func qaPromptModel(t *testing.T, r promptRequest, cols, rows int) *promptModel {
	t.Helper()
	m := newPromptModel(r, cols, rows)
	t.Cleanup(m.Close)
	return m
}
func qaPromptPending(t *testing.T, m *promptModel, e terminal.Event) {
	t.Helper()
	if r, done := m.Handle(e); done {
		clear(r.secret)
		t.Fatalf("event %s implicitly completed prompt: %+v", e.Kind, r)
	}
}
func qaPromptSubmit(t *testing.T, m *promptModel) promptReply {
	t.Helper()
	r, done := m.Handle(terminal.Event{Kind: "enter"})
	if !done {
		t.Fatal("explicit Enter did not complete eligible prompt")
	}
	if r.err != nil {
		t.Fatalf("prompt submit failed: %v", r.err)
	}
	return r
}
func qaPromptFrame(t *testing.T, lines []string, cols, rows int) string {
	t.Helper()
	if len(lines) > max(0, rows) {
		t.Errorf("frame has %d lines, budget %d", len(lines), rows)
	}
	for _, line := range lines {
		if !utf8.ValidString(line) || uniseg.StringWidth(line) > max(0, cols-1) {
			t.Errorf("invalid or oversized frame line: %q", line)
		}
		for _, r := range line {
			if unicode.IsControl(r) || r >= 0x202a && r <= 0x202e || r >= 0x2066 && r <= 0x2069 || r == 0x061c || r == 0x200e || r == 0x200f {
				t.Errorf("foreign control reached renderer U+%04X", r)
			}
		}
	}
	return strings.Join(lines, "\n")
}

func TestQAUIPromptChooseSearchStableIDsAndDefensiveCopy(t *testing.T) {
	r := qaPromptRequest("choose", "Project/account")
	r.choices = []terminal.Choice{{ID: "alpha", Label: "Alpha"}, {ID: "quiet-id", Label: "Quiet account"}, {ID: "beta", Label: "Beta"}}
	m := qaPromptModel(t, r, 80, 24)
	r.choices[1] = terminal.Choice{ID: "wrong", Label: "Wrong"}
	qaPromptPending(t, m, terminal.Event{Kind: "text", Text: "Q"})
	frame := qaPromptFrame(t, m.Render(nil), 80, 24)
	if !strings.Contains(frame, "Quiet account") || strings.Contains(frame, "Alpha") {
		t.Errorf("case insensitive searchable choice missing/mixed: %q", frame)
	}
	if got := qaPromptSubmit(t, m); got.choiceID != "quiet-id" {
		t.Errorf("search returned unstable/mutated ID: %+v", got)
	}
}
func TestQAUIPromptChooseNavigationEmptyAndPaste(t *testing.T) {
	for _, mode := range []string{"arrows", "no-match", "paste"} {
		t.Run(mode, func(t *testing.T) {
			r := qaPromptRequest("choose", "Pick")
			r.choices = []terminal.Choice{{ID: "a", Label: "Alpha"}, {ID: "q", Label: "Quiet"}}
			m := qaPromptModel(t, r, 80, 24)
			switch mode {
			case "arrows":
				qaPromptPending(t, m, terminal.Event{Kind: "down"})
			case "no-match":
				qaPromptPending(t, m, terminal.Event{Kind: "text", Text: "missing"})
				qaPromptPending(t, m, terminal.Event{Kind: "enter"})
				if !strings.Contains(strings.ToLower(strings.Join(m.Render(nil), "\n")), "no match") {
					t.Error("empty result not distinguished")
				}
				return
			case "paste":
				qaPromptPending(t, m, terminal.Event{Kind: "paste", Text: "q\r\n\x03"})
			}
			if got := qaPromptSubmit(t, m); got.choiceID != "q" {
				t.Errorf("navigation/paste selected wrong ID: %+v", got)
			}
		})
	}
}
func TestQAUIPromptTextEditableDefaultPasteAndGraphemeBackspace(t *testing.T) {
	r := qaPromptRequest("text", "Project path")
	r.defaultText = "draft"
	m := qaPromptModel(t, r, 80, 24)
	qaPromptPending(t, m, terminal.Event{Kind: "text", Text: "q"})
	qaPromptPending(t, m, terminal.Event{Kind: "paste", Text: "\r\n界👩‍💻\x03\x1a"})
	qaPromptPending(t, m, terminal.Event{Kind: "backspace"})
	m.Resize(40, 10)
	qaPromptFrame(t, m.Render(nil), 40, 10)
	m.Resize(80, 24)
	if got := qaPromptSubmit(t, m); got.text != "draftq界" {
		t.Errorf("default/paste/grapheme edit lost or split data: %q", got.text)
	}
}
func TestQAUIPromptEscapeCancelsEachModal(t *testing.T) {
	for _, kind := range []string{"choose", "text", "secret", "confirm"} {
		t.Run(kind, func(t *testing.T) {
			r := qaPromptRequest(kind, "Cancel safely")
			r.choices = []terminal.Choice{{ID: "a", Label: "Alpha"}}
			m := qaPromptModel(t, r, 80, 24)
			reply, done := m.Handle(terminal.Event{Kind: "escape"})
			var end *terminal.ExitError
			if !done || !errors.As(reply.err, &end) || end.Code != 0 || reply.confirmed || len(reply.secret) != 0 {
				t.Errorf("modal Escape did not return clean local cancellation: %+v done=%v", reply, done)
			}
		})
	}
}
func TestQAUIPromptTinyLayoutsCannotSubmitHiddenFields(t *testing.T) {
	for _, kind := range []string{"choose", "text", "secret", "confirm"} {
		t.Run(kind, func(t *testing.T) {
			r := qaPromptRequest(kind, "Entire warning must be visible")
			r.choices = []terminal.Choice{{ID: "a", Label: "Alpha"}}
			m := qaPromptModel(t, r, 20, 5)
			qaPromptPending(t, m, terminal.Event{Kind: "text", Text: "y"})
			qaPromptPending(t, m, terminal.Event{Kind: "enter"})
			if frame := qaPromptFrame(t, m.Render(nil), 20, 5); !strings.Contains(strings.ToLower(frame), "too small") {
				t.Errorf("tiny layout omitted resize guidance: %q", frame)
			}
			m.Resize(1, 1)
			qaPromptFrame(t, m.Render(nil), 1, 1)
			qaPromptPending(t, m, terminal.Event{Kind: "enter"})
			_, done := m.Handle(terminal.Event{Kind: "escape"})
			if !done {
				t.Error("tiny layout made modal impossible to cancel")
			}
		})
	}
}

type qaPromptSpyStyler struct {
	t      *testing.T
	secret string
	cols   int
}

func (s qaPromptSpyStyler) Paint(_ terminal.Role, text string) string {
	if strings.Contains(text, s.secret) {
		s.t.Error("private secret passed through Styler")
	}
	qaPromptFrame(s.t, []string{text}, s.cols, 1)
	return text
}
func qaPromptBytes(v reflect.Value) [][]byte {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return qaPromptBytes(v.Elem())
	}
	if v.Kind() == reflect.Struct {
		var out [][]byte
		for i := 0; i < v.NumField(); i++ {
			out = append(out, qaPromptBytes(v.Field(i))...)
		}
		return out
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		return [][]byte{v.Bytes()}
	}
	return nil
}

func qaPromptStrings(v reflect.Value) []string {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return qaPromptStrings(v.Elem())
	}
	if v.Kind() == reflect.Struct {
		var out []string
		for i := 0; i < v.NumField(); i++ {
			out = append(out, qaPromptStrings(v.Field(i))...)
		}
		return out
	}
	if v.Kind() == reflect.String {
		return []string{v.String()}
	}
	return nil
}
func TestQAUIPromptSecretPrivateReplyAndControllerZeroing(t *testing.T) {
	const secret = "synthetic-qa-private-token"
	r := qaPromptRequest("secret", "Credential")
	m := qaPromptModel(t, r, 80, 24)
	qaPromptPending(t, m, terminal.Event{Kind: "paste", Text: "synthetic-qa-private-\ntoken\r\x03"})
	for _, field := range qaPromptStrings(reflect.ValueOf(m)) {
		if strings.Contains(field, secret) {
			t.Error("secret retained in ordinary string field")
		}
	}
	for _, styler := range []terminal.Styler{nil, qaPromptSpyStyler{t: t, secret: secret, cols: 80}} {
		if frame := qaPromptFrame(t, m.Render(styler), 80, 24); strings.Contains(frame, secret) || strings.Contains(frame, "synthetic-qa-private") {
			t.Errorf("secret leaked into ordinary frame: %q", frame)
		}
	}
	buffers := qaPromptBytes(reflect.ValueOf(m))
	found := false
	for _, buf := range buffers {
		found = found || string(buf) == secret
	}
	if !found {
		t.Error("secret input is not held in the private clearable byte buffer")
	}
	reply := qaPromptSubmit(t, m)
	defer clear(reply.secret)
	if string(reply.secret) != secret || reply.text != "" || reply.choiceID != "" {
		t.Errorf("secret reply absent or copied into ordinary text fields: %+v", reply)
	}
	m.Close()
	for _, buf := range buffers {
		for _, b := range buf {
			if b != 0 {
				t.Error("Close retained secret in controller backing memory")
				break
			}
		}
	}
	if string(reply.secret) != secret {
		t.Error("controller Close destroyed caller-owned secret reply")
	}
	if frame := strings.Join(m.Render(nil), "\n"); strings.Contains(frame, secret) {
		t.Error("closed form retains secret")
	}
}

func TestQAUIPromptSecretBackspaceAndCanceledFormEraseBytes(t *testing.T) {
	for _, mode := range []string{"backspace", "caller-canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			r := qaPromptRequest("secret", "Credential")
			r.ctx = ctx
			m := qaPromptModel(t, r, 80, 24)
			qaPromptPending(t, m, terminal.Event{Kind: "text", Text: "privateq"})
			var original []byte
			for _, buf := range qaPromptBytes(reflect.ValueOf(m)) {
				if string(buf) == "privateq" {
					original = buf
					break
				}
			}
			if original == nil {
				t.Fatal("secret is not in an owned clearable buffer")
			}
			if mode == "backspace" {
				qaPromptPending(t, m, terminal.Event{Kind: "backspace"})
				if original[7] != 0 {
					t.Error("backspace retained removed secret byte in backing storage")
				}
			} else {
				want := errors.New("caller canceled active secret form")
				cancel(want)
				reply, done := m.Handle(terminal.Event{Kind: "enter"})
				if !done || !errors.Is(reply.err, want) || len(reply.secret) != 0 || reply.text != "" {
					clear(reply.secret)
					t.Errorf("canceled form returned token or lost cause: done=%v err=%v", done, reply.err)
				}
			}
			m.Close()
			for _, b := range original {
				if b != 0 {
					t.Error("Close retained previously entered secret bytes")
					break
				}
			}
		})
	}
}
func TestQAUIPromptConfirmFullWarningReviewAndExplicitEnter(t *testing.T) {
	warning := "FIRST WARNING " + strings.Repeat("long scoped account/path warning ", 35) + " LAST WARNING"
	m := qaPromptModel(t, qaPromptRequest("confirm", warning), 40, 8)
	qaPromptPending(t, m, terminal.Event{Kind: "text", Text: "y"})
	qaPromptPending(t, m, terminal.Event{Kind: "enter"})
	visited := map[string]bool{}
	for i := 0; i < 100; i++ {
		frame := qaPromptFrame(t, m.Render(nil), 40, 8)
		visible := strings.Join(strings.Fields(frame), " ")
		if strings.Contains(visible, "FIRST WARNING") {
			visited["first"] = true
		}
		if strings.Contains(visible, "LAST WARNING") {
			visited["last"] = true
			break
		}
		qaPromptPending(t, m, terminal.Event{Kind: "down"})
	}
	if !visited["first"] || !visited["last"] {
		t.Fatal("scroll did not expose entire sanitized warning")
	}
	qaPromptPending(t, m, terminal.Event{Kind: "text", Text: "y"})
	if got := qaPromptSubmit(t, m); !got.confirmed {
		t.Error("reviewed explicit affirmative did not confirm")
	}
}
func TestQAUIPromptConfirmPasteCannotArmAndDefaultIsNegative(t *testing.T) {
	for _, mode := range []string{"default", "paste", "typed-yes"} {
		t.Run(mode, func(t *testing.T) {
			m := qaPromptModel(t, qaPromptRequest("confirm", "Apply this scoped change?"), 80, 24)
			m.Render(nil)
			if mode == "paste" {
				qaPromptPending(t, m, terminal.Event{Kind: "paste", Text: "y\r\nConfirm"})
			}
			if mode == "typed-yes" {
				qaPromptPending(t, m, terminal.Event{Kind: "text", Text: "y"})
			}
			if got := qaPromptSubmit(t, m); got.confirmed != (mode == "typed-yes") {
				t.Errorf("confirmation requires reviewed typed choice and Enter: %+v", got)
			}
		})
	}
}
func TestQAUIPromptForeignSpansSanitizedBeforeStylesAndClipped(t *testing.T) {
	for _, cols := range []int{120, 80, 40, 20, 1} {
		t.Run(strconv.Itoa(cols), func(t *testing.T) {
			payload := "Visible \x1b]52;c;SECRET\a\x1bPmalicious\x1b\\\u202e\u2067" + string([]byte{0xff}) + strings.Repeat("Ω\u0301界👩‍💻", 40)
			r := qaPromptRequest("choose", payload)
			r.choices = []terminal.Choice{{ID: "a", Label: payload}}
			m := qaPromptModel(t, r, cols, 12)
			frame := qaPromptFrame(t, m.Render(qaPromptSpyStyler{t: t, secret: "SECRET", cols: cols}), cols, 12)
			if len(m.Render(nil)) == 0 {
				t.Fatal("prompt produced no frame")
			}
			if cols >= 40 && !strings.Contains(frame, "Visible") {
				t.Errorf("sanitizer dropped legitimate visible title: %q", frame)
			}
			if strings.Contains(frame, "SECRET") || strings.Contains(frame, "malicious") {
				t.Errorf("terminal escape payload survived sanitization: %q", frame)
			}
			for _, line := range m.Render(nil) {
				if strings.Contains(line, "Ω") && strings.Count(line, "Ω") != strings.Count(line, "Ω\u0301") {
					t.Error("clipping split combining grapheme")
				}
				if strings.Contains(line, "👩") && strings.Count(line, "👩") != strings.Count(line, "👩‍💻") {
					t.Error("clipping split emoji grapheme")
				}
			}
		})
	}
}
func TestQAUIPromptInputBudgetsFailWithoutReturningTruncatedValues(t *testing.T) {
	for _, kind := range []string{"choose", "text", "secret"} {
		t.Run(kind, func(t *testing.T) {
			r := qaPromptRequest(kind, "Bounded input")
			r.choices = []terminal.Choice{{ID: "a", Label: "Alpha"}}
			m := qaPromptModel(t, r, 80, 24)
			limit := 16384
			if kind == "choose" {
				limit = 256
			}
			reply, done := m.Handle(terminal.Event{Kind: "paste", Text: strings.Repeat("x", limit+1)})
			if !done || reply.err == nil || reply.text != "" || len(reply.secret) != 0 || reply.choiceID != "" {
				t.Errorf("oversize input was not rejected safely: done=%v err=%v ordinaryLen=%d secretLen=%d", done, reply.err, len(reply.text), len(reply.secret))
			}
		})
	}
}

func TestQAUIPromptBridgeRoutesTypedRequestsAndCopiesChoices(t *testing.T) {
	for _, kind := range []string{"choose", "text", "secret", "confirm"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b := newPromptBridge(ctx)
			if cap(b.requests) != 1 {
				t.Error("prompt request queue must be capacity1")
			}
			choices := []terminal.Choice{{ID: "stable", Label: "Stable"}}
			out := make(chan promptReply, 1)
			go func() {
				var r promptReply
				switch kind {
				case "choose":
					r.choiceID, r.err = b.Choose(ctx, "TITLE", choices)
				case "text":
					r.text, r.err = b.Text(ctx, "TITLE", "draft")
				case "secret":
					r.secret, r.err = b.Secret(ctx, "TITLE")
				case "confirm":
					r.confirmed, r.err = b.Confirm(ctx, "TITLE")
				}
				out <- r
			}()
			var req promptRequest
			select {
			case req = <-b.requests:
			case got := <-out:
				clear(got.secret)
				t.Fatalf("bridge returned without publishing request: %v", got.err)
			case <-ctx.Done():
				t.Fatal("broker did not publish bounded request")
			}
			if req.kind != kind || req.title != "TITLE" || req.ctx == nil || cap(req.reply) != 1 {
				t.Errorf("typed prompt request lost routing/context/capacity: %+v", req)
			}
			if kind == "choose" {
				choices[0] = terminal.Choice{ID: "wrong", Label: "Wrong"}
				if req.choices[0].ID != "stable" {
					t.Error("bridge retained caller-owned choices")
				}
			}
			if kind == "text" && req.defaultText != "draft" {
				t.Error("bridge lost editable default")
			}
			req.reply <- promptReply{choiceID: "stable", text: "edited", secret: []byte("private-reply"), confirmed: true}
			select {
			case got := <-out:
				defer clear(got.secret)
				if got.err != nil || kind == "choose" && got.choiceID != "stable" || kind == "text" && got.text != "edited" || kind == "secret" && string(got.secret) != "private-reply" || kind == "confirm" && !got.confirmed {
					t.Errorf("bridge returned wrong typed reply: %+v", got)
				}
			case <-ctx.Done():
				t.Fatal("broker reply blocked")
			}
		})
	}
}
func TestQAUIPromptBridgeCancellationWhileQueueAndReplyBlocked(t *testing.T) {
	for _, mode := range []string{"caller-full-queue", "caller-reply", "bridge-reply"} {
		t.Run(mode, func(t *testing.T) {
			base, stopBase := context.WithCancelCause(context.Background())
			defer stopBase(nil)
			caller, stopCaller := context.WithCancelCause(context.Background())
			defer stopCaller(nil)
			b := newPromptBridge(base)
			if mode == "caller-full-queue" {
				b.requests <- qaPromptRequest("text", "occupied")
			}
			out := make(chan error, 1)
			go func() { _, err := b.Text(caller, "Canceled", "draft"); out <- err }()
			if mode != "caller-full-queue" {
				select {
				case <-b.requests:
				case err := <-out:
					t.Fatalf("bridge returned before request/cancellation: %v", err)
				case <-time.After(time.Second):
					t.Fatal("request never queued")
				}
			}
			want := errors.New("synthetic exact cancellation cause")
			if mode == "bridge-reply" {
				stopBase(want)
			} else {
				stopCaller(want)
			}
			select {
			case got := <-out:
				if !errors.Is(got, want) {
					t.Errorf("broker lost cancellation cause: %v", got)
				}
			case <-time.After(time.Second):
				t.Fatal("broker did not join after cancellation")
			}
		})
	}
}
