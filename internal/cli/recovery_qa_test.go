package cli_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestQARecoveryCLIReadOnlyOfflineReview(t *testing.T) {
	r, _ := qaRunActivity(t, "", "activity", "review", "--non-interactive")
	envelope(t, r, 0, "")
}
func TestQARecoveryCLIValidationAndNotFoundAreFiniteJSON(t *testing.T) {
	id := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	for _, tc := range []struct {
		name string
		args []string
		exit int
		code string
	}{
		{"preview-not-found", []string{"activity", "preview", id, "--discard-tail"}, 5, "uncertainty_not_found"},
		{"resolve-choice-required", []string{"activity", "resolve", id, "--if-revision", "1", "--yes"}, 2, "input_required"},
		{"resolve-confirmation", []string{"activity", "resolve", id, "--discard-tail", "--if-revision", "1"}, 6, "confirmation_required"},
		{"interrupt-generation", []string{"activity", "interrupt", id, "--if-revision", "1", "--yes"}, 2, "input_required"},
		{"offset-end", []string{"activity", "preview", id, "--end", "2026-10-02T10:00:00+01:00"}, 2, "validation"},
		{"single-digit-hour", []string{"activity", "preview", id, "--end", "2026-10-02T1:00:00Z"}, 2, "validation"},
		{"comma-fraction", []string{"activity", "preview", id, "--end", "2026-10-02T10:00:00,5Z"}, 2, "validation"},
		{"both-choices", []string{"activity", "preview", id, "--end", "2026-10-02T10:00:00Z", "--discard-tail"}, 2, "validation"},
		{"clock-only-end", []string{"activity", "preview", id, "--end", "10:00"}, 2, "validation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append(tc.args, "--non-interactive")
			r, _ := qaRunActivity(t, "", args...)
			envelope(t, r, tc.exit, tc.code)
		})
	}
}
func TestQARecoveryCLITimestampEndDoesNotBreakHarvestClockEnd(t *testing.T) {
	a := &fakeAPI{timestamps: true}
	r := run(t, a, "time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--start", "09:15", "--end", "10:45", "--json")
	envelope(t, r, 0, "")
}

func TestQARecoveryCLIPreviewResolveAndReviewRemainOffline(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "state", "activity.json")
	base := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	seconds := int64(0)
	clockFailed := false
	s := activity.New(activity.Options{Path: path, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		epoch := "qa-boot"
		n := strconv.FormatInt(seconds*int64(time.Second), 10)
		sample := activity.ClockSample{Capability: "available", WallUTC: base.Add(time.Duration(seconds) * time.Second), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}
		if clockFailed {
			return sample, errors.New("synthetic unavailable")
		}
		return sample, nil
	})})
	binding, err := s.Link(context.Background(), activity.LinkInput{Path: t.TempDir(), ProjectID: "100", TaskID: "200", AccountID: "11", Timezone: "UTC", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}, activity.LinkDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return &qaCLILinkAPI{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	event := activity.Event{ContractVersion: 1, Actor: activity.ActorKey{ComputerID: *snap.ComputerID, Source: "manual-test", SessionID: "cli-recovery", AgentID: "A"}, Generation: "1", Sequence: "1", EventID: "cli-work", Kind: "work", BindingID: binding.Binding.ID, BindingRevision: binding.Binding.Revision}
	if _, err := s.Ingest(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	seconds = 10
	event.Sequence = "2"
	event.EventID = "cli-observe"
	event.Kind = "observe_work"
	event.BindingID = ""
	event.BindingRevision = ""
	if _, err := s.Ingest(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	seconds = 20
	clockFailed = true
	event.Sequence = "3"
	event.EventID = "cli-stop"
	event.Kind = "finish"
	if _, err := s.Ingest(context.Background(), event); err == nil {
		t.Fatal("fixture clock loss accepted")
	}
	seconds = 30
	clockFailed = false
	snap, err = s.Status(context.Background())
	if err != nil || len(snap.Uncertainties) != 1 {
		t.Fatalf("fixture uncertainty %v %+v", err, snap)
	}
	u := snap.Uncertainties[0]
	store := &fakeStore{}
	d := cli.Dependencies{Activity: s, Store: store, ConfigPath: filepath.Join(root, "missing-config"), Getenv: func(string) string { t.Fatal("recovery read account environment"); return "" }, NewProvider: func(string, string) harvest.Provider { t.Fatal("recovery constructed provider"); return nil }}
	runLocal := func(args ...string) {
		t.Helper()
		var out, stderr bytes.Buffer
		args = append(args, "--non-interactive")
		code := cli.Run(context.Background(), args, qaNeverRead{t}, &out, &stderr, d)
		envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 0, "")
	}
	end := base.Add(15 * time.Second).Format(time.RFC3339Nano)
	runLocal("activity", "review", "--account", "11", "--project", "100")
	runLocal("activity", "preview", u.ID, "--end", end)
	runLocal("activity", "resolve", u.ID, "--end", end, "--if-revision", u.Revision, "--yes", "--reason", "verified", "--request-id", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	runLocal("activity", "review")
	final, err := s.Status(context.Background())
	if err != nil || len(final.ClosedIntervals) != 1 || final.ClosedIntervals[0].DurationNS != "15000000000" {
		t.Fatalf("CLI recovery did not produce asserted interval %+v %v", final, err)
	}
	if store.gets+store.sets+store.deletes != 0 {
		t.Fatal("recovery accessed credential store")
	}
}
