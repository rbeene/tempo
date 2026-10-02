package setup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
)

type qaPreparePrompt struct {
	*qaSetupPrompt
	beforeConfirm func()
	title         string
}

type qaPrepareCanceledPrompt struct {
	*qaSetupPrompt
	started chan struct{}
}

func (p *qaPrepareCanceledPrompt) Choose(ctx context.Context, _ string, _ []terminal.Choice) (string, error) {
	close(p.started)
	<-ctx.Done()
	return "", context.Cause(ctx)
}

func (p *qaPreparePrompt) Confirm(ctx context.Context, title string) (bool, error) {
	p.title = title
	if p.beforeConfirm != nil {
		p.beforeConfirm()
	}
	return p.qaSetupPrompt.Confirm(ctx, title)
}

func qaPrepareAbsent(t *testing.T, state string) {
	t.Helper()
	if _, err := os.Stat(filepath.Dir(state)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prepare initialized activity directory: %v", err)
	}
}

func TestQAPrepareLinkResolvesConfirmedIntentWithoutWriting(t *testing.T) {
	s, state, in := qaGuidedLink(t)
	in.AccountID = ""
	original := in
	p := &qaPreparePrompt{qaSetupPrompt: &qaSetupPrompt{t: t, choices: []string{"3", "5"}, confirm: true}}
	prepared, err := s.PrepareLink(context.Background(), in, p)
	if err != nil {
		t.Fatal(err)
	}
	want := activity.LinkInput{Path: in.Path, RequestID: in.RequestID, AccountID: "11", ProjectID: "3", TaskID: "5", Timezone: "UTC"}
	if prepared != want || in != original {
		t.Fatalf("prepare did not retain exact value intent: got %+v want %+v caller %+v", prepared, want, in)
	}
	if p.confirmCalls != 1 || p.chooseCalls != 2 || p.textCalls != 1 || len(p.choices) != 0 {
		t.Fatalf("guided preparation skipped required choices/consent: %+v", p.qaSetupPrompt)
	}
	for _, span := range []string{"account 11", "project 3", "task 5", "UTC"} {
		if !strings.Contains(p.title, span) {
			t.Errorf("confirmation omitted resolved target %q: %q", span, p.title)
		}
	}
	qaPrepareAbsent(t, state)
	// Only Commit can create the mapping and receipt from the returned value.
	result, err := s.CommitLink(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequestID != prepared.RequestID || result.Binding.Attribution != (activity.Attribution{AccountID: "11", UserID: "2", ProjectID: "3", TaskID: "5", Timezone: "UTC"}) {
		t.Fatalf("commit changed prepared identity/attribution: %+v", result)
	}
}

func TestQAPrepareLinkCancellationReturnsNoIntentOrWrites(t *testing.T) {
	for _, stage := range []string{"choose", "text", "confirm"} {
		t.Run(stage, func(t *testing.T) {
			s, state, in := qaGuidedLink(t)
			p := &qaSetupPrompt{t: t, choices: []string{"3", "5"}, cancelAt: stage}
			prepared, err := s.PrepareLink(context.Background(), in, p)
			var ended *terminal.ExitError
			if !errors.As(err, &ended) || ended.Code != 0 || prepared != (activity.LinkInput{}) {
				t.Fatalf("canceled prepare returned usable intent or wrong cause: %+v %v", prepared, err)
			}
			if stage == "choose" && p.chooseCalls == 0 || stage == "text" && p.textCalls == 0 || stage == "confirm" && p.confirmCalls == 0 {
				t.Fatal("requested cancellation stage was not reached")
			}
			qaPrepareAbsent(t, state)
		})
	}
}

func TestQAPrepareLinkCanceledCallerJoinsPromptAndKeepsCause(t *testing.T) {
	s, state, in := qaGuidedLink(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	p := &qaPrepareCanceledPrompt{qaSetupPrompt: &qaSetupPrompt{t: t}, started: make(chan struct{})}
	type outcome struct {
		input activity.LinkInput
		err   error
	}
	done := make(chan outcome, 1)
	go func() { prepared, err := s.PrepareLink(ctx, in, p); done <- outcome{prepared, err} }()
	select {
	case <-p.started:
	case result := <-done:
		t.Fatalf("prepare returned before joining any prompt: %+v %v", result.input, result.err)
	case <-time.After(time.Second):
		t.Fatal("guided preparation never reached prompt")
	}
	cause := &terminal.ExitError{Code: 143}
	cancel(cause)
	select {
	case result := <-done:
		if !errors.Is(result.err, cause) || result.input != (activity.LinkInput{}) {
			t.Fatalf("canceled prepare lost cause or returned dispatchable input: %+v %v", result.input, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("preparation retained canceled prompt work")
	}
	qaPrepareAbsent(t, state)
}

func TestQAPrepareLinkBypassesRemainOfflineAndKeepRelativePath(t *testing.T) {
	for _, mode := range []string{"nil-prompter", "fully-explicit"} {
		t.Run(mode, func(t *testing.T) {
			// Nil services make any discovery/auth/state read a test failure.
			s := New(Options{})
			in := activity.LinkInput{Path: "./relative/project", AccountID: "11", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", IfRevision: "9"}
			var p terminal.Prompter
			if mode == "fully-explicit" {
				in.ProjectID, in.TaskID, in.Timezone = "3", "5", "UTC"
				p = &qaSetupPrompt{t: t}
			}
			prepared, err := s.PrepareLink(context.Background(), in, p)
			if err != nil || prepared != in {
				t.Fatalf("bypass rewrote CLI-relative input or required dependencies: %+v %v", prepared, err)
			}
		})
	}
	prepared, err := New(Options{}).PrepareLink(context.Background(), activity.LinkInput{Path: "./relative/project"}, nil)
	if err != nil || !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(prepared.RequestID) || prepared.Path != "./relative/project" {
		t.Fatalf("prepare did not generate one valid request identity while keeping path: %+v %v", prepared, err)
	}
}

func TestQAPrepareLinkFreezesObservedRevisionBeforeConcurrentChange(t *testing.T) {
	s, state, in := qaGuidedLink(t)
	initial := in
	initial.ProjectID, initial.TaskID, initial.Timezone = "3", "4", "UTC"
	first, err := s.Link(context.Background(), initial, nil)
	if err != nil {
		t.Fatal(err)
	}
	in.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	p := &qaPreparePrompt{qaSetupPrompt: &qaSetupPrompt{t: t, choices: []string{"3"}, confirm: true}}
	var changed activity.BindingResult
	p.beforeConfirm = func() {
		other := initial
		other.TaskID, other.IfRevision, other.RequestID = "5", first.Binding.Revision, "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
		changed, err = s.Link(context.Background(), other, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	prepared, err := s.PrepareLink(context.Background(), in, p)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.IfRevision != first.Binding.Revision || prepared.TaskID != "4" || prepared.Timezone != "UTC" || prepared.RequestID != in.RequestID {
		t.Fatalf("prepare substituted newly changed revision/attribution: %+v first %+v", prepared, first)
	}
	before, err := os.ReadFile(state)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CommitLink(context.Background(), prepared)
	var local *activity.Error
	if !errors.As(err, &local) || local.Code != "revision_conflict" {
		t.Fatalf("stale prepared intent silently committed: %v", err)
	}
	after, err := os.ReadFile(state)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("revision conflict changed activity state")
	}
	listed, err := s.options.Activity.ListBindings(context.Background())
	if err != nil || len(listed.Bindings) != 1 || listed.Bindings[0].Revision != changed.Binding.Revision || listed.Bindings[0].Attribution.TaskID != "5" {
		t.Fatalf("prepare/failed commit rewrote concurrent mapping: %+v %v", listed, err)
	}
}

func TestQACommitPreparedLinkReceiptReplaySkipsPathAndAuth(t *testing.T) {
	s, _, in := qaGuidedLink(t)
	// Project may disappear after dispatch without removing the durable receipt.
	in.Path = t.TempDir()
	state := filepath.Join(t.TempDir(), "activity", "state")
	s.options.Activity = activity.New(activity.Options{Path: state})
	p := &qaSetupPrompt{t: t, choices: []string{"3", "5"}, confirm: true}
	prepared, err := s.PrepareLink(context.Background(), in, p)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.CommitLink(context.Background(), prepared)
	if err != nil || first.RequestID != in.RequestID || first.Binding.ID == "" {
		t.Fatalf("prepared commit missing shared receipt: %+v %v", first, err)
	}
	if err := os.Remove(in.Path); err != nil {
		t.Fatal(err)
	}
	forbidden := auth.NewService(auth.Options{ConfigPath: filepath.Join(t.TempDir(), "cfg"), LockPath: filepath.Join(t.TempDir(), "lock"), Getenv: func(string) string { t.Error("replay resolved auth environment"); return "" }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Error("replay accessed credentials")
		return auth.NativeReply{}, errors.New("forbidden native call")
	}), NewProvider: func(string, string) harvest.Provider { t.Error("replay constructed provider"); return nil }})
	restarted := New(Options{Auth: forbidden, Activity: activity.New(activity.Options{Path: state})})
	replay, err := restarted.CommitLink(context.Background(), prepared)
	if err != nil || !reflect.DeepEqual(first, replay) {
		t.Fatalf("exact retained replay rediscovered path or changed receipt: %+v %v", replay, err)
	}
	list, err := restarted.options.Activity.ListBindings(context.Background())
	if err != nil || len(list.Bindings) != 1 || list.Bindings[0].Revision != "1" {
		t.Fatalf("same-ID replay duplicated link: %+v %v", list, err)
	}
	altered := prepared
	altered.TaskID = "4"
	_, err = restarted.CommitLink(context.Background(), altered)
	var local *activity.Error
	if !errors.As(err, &local) || local.Code != "request_conflict" {
		t.Fatalf("changed intent reused committed request identity: %v", err)
	}
}
