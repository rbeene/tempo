package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/harvest"
)

const qaLinkRequest = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

type qaLinkProvider struct {
	t           *testing.T
	accounts    []harvest.Object
	user        harvest.Object
	assignments []harvest.Object
	calls       []string
	listErr     error
	beforeList  func()
}

func qaNewLinkProvider(t *testing.T) *qaLinkProvider {
	return &qaLinkProvider{t: t, accounts: []harvest.Object{{"id": json.Number("1"), "product": "harvest"}}, user: harvest.Object{"id": json.Number("2"), "is_active": true}, assignments: []harvest.Object{{"is_active": true, "project": harvest.Object{"id": json.Number("3")}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("4")}}}}}}
}
func (p *qaLinkProvider) Accounts(context.Context) ([]harvest.Object, error) {
	p.calls = append(p.calls, "accounts")
	return p.accounts, nil
}
func (p *qaLinkProvider) Get(_ context.Context, path string) (harvest.Object, error) {
	p.calls = append(p.calls, path)
	if path != "/users/me" {
		p.t.Fatalf("unexpected GET %s", path)
	}
	return p.user, nil
}
func (p *qaLinkProvider) List(_ context.Context, path string, _ url.Values) ([]harvest.Object, error) {
	p.calls = append(p.calls, path)
	if path != "/users/me/project_assignments" {
		p.t.Fatalf("unexpected List %s", path)
	}
	if p.beforeList != nil {
		p.beforeList()
	}
	return p.assignments, p.listErr
}
func (p *qaLinkProvider) Create(context.Context, string, harvest.Object) (harvest.Object, error) {
	p.t.Fatal("link performed remote Create")
	return nil, nil
}
func (p *qaLinkProvider) Update(context.Context, string, harvest.Object) (harvest.Object, error) {
	p.t.Fatal("link performed remote Update")
	return nil, nil
}
func (p *qaLinkProvider) Delete(context.Context, string) error {
	p.t.Fatal("link performed remote Delete")
	return nil
}
func qaLinkDeps(t *testing.T, p *qaLinkProvider) LinkDependencies {
	return LinkDependencies{ResolveAccount: func(context.Context) (string, error) {
		t.Fatal("explicit account unexpectedly resolved from config")
		return "", nil
	}, NewProvider: func(_ context.Context, account string) (harvest.Provider, error) {
		if account != "1" {
			t.Fatalf("provider account=%s", account)
		}
		return p, nil
	}}
}
func qaLinkInput(t *testing.T) LinkInput {
	return LinkInput{ProjectID: "3", TaskID: "4", AccountID: "1", Timezone: "UTC", Path: t.TempDir(), RequestID: qaLinkRequest}
}
func qaLinkService(t *testing.T) (*Service, string) {
	path := filepath.Join(t.TempDir(), "absent", "activity.json")
	return New(Options{Path: path}), path
}
func qaAbsentLinkState(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed/read-only link initialized state: %v", err)
	}
}

func TestQABindingEmptyListNeverBootstraps(t *testing.T) {
	s, path := qaLinkService(t)
	r, err := s.ListBindings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.ContractVersion != 1 || r.SnapshotRevision != "0" || r.Bindings == nil || len(r.Bindings) != 0 {
		t.Fatalf("empty list=%+v", r)
	}
	qaAbsentLinkState(t, path)
}
func TestQABindingSuccessfulLinkBootstrapsAndSurvivesRestart(t *testing.T) {
	s, path := qaLinkService(t)
	in := qaLinkInput(t)
	p := qaNewLinkProvider(t)
	r, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(in.Path)
	if err != nil {
		t.Fatal(err)
	}
	want := Attribution{AccountID: "1", UserID: "2", ProjectID: "3", TaskID: "4", Timezone: "UTC"}
	if !r.Changed || r.RequestID != in.RequestID || r.ContractVersion != 1 || !validUUID(r.Binding.ID) || r.Binding.Revision != "1" || r.Binding.Kind != "directory" || r.Binding.Locator != canonical || r.Binding.Attribution != want {
		t.Fatalf("bad created binding: %+v", r)
	}
	snapshot, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ComputerID == nil || !validUUID(*snapshot.ComputerID) {
		t.Fatalf("missing initialized identity: %+v", snapshot)
	}
	_, _, database, err := sqliteLocation(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(filepath.Dir(path), database))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("SQLite state privacy: %v %v", info, err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("default Link created JSON selector", err)
	}
	reloaded := New(Options{Path: path})
	list, err := reloaded.ListBindings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Bindings) != 1 || !reflect.DeepEqual(list.Bindings[0], r.Binding) {
		t.Fatalf("restart lost binding: %+v", list)
	}
}
func TestQABindingValidationNeverInitializesState(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		alter      func(*LinkInput, *qaLinkProvider)
	}{
		{"account-inaccessible", "forbidden", func(_ *LinkInput, p *qaLinkProvider) { p.accounts = nil }},
		{"inactive-user", "forbidden", func(_ *LinkInput, p *qaLinkProvider) { p.user["is_active"] = false }},
		{"inactive-assignment", "validation", func(_ *LinkInput, p *qaLinkProvider) { p.assignments[0]["is_active"] = false }},
		{"wrong-task", "validation", func(i *LinkInput, _ *qaLinkProvider) { i.TaskID = "999" }},
		{"missing-project", "input_required", func(i *LinkInput, _ *qaLinkProvider) { i.ProjectID = "" }},
		{"missing-timezone", "input_required", func(i *LinkInput, _ *qaLinkProvider) { i.Timezone = "" }},
		{"machine-timezone", "validation", func(i *LinkInput, _ *qaLinkProvider) { i.Timezone = "Local" }},
		{"timezone-double-slash", "validation", func(i *LinkInput, _ *qaLinkProvider) { i.Timezone = "America//New_York" }},
		{"timezone-dot-alias", "validation", func(i *LinkInput, _ *qaLinkProvider) { i.Timezone = "America/./New_York" }},
		{"initial-revision-without-binding", "revision_conflict", func(i *LinkInput, _ *qaLinkProvider) { i.IfRevision = "1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, path := qaLinkService(t)
			in := qaLinkInput(t)
			p := qaNewLinkProvider(t)
			tc.alter(&in, p)
			_, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
			qaCode(t, err, tc.code)
			qaAbsentLinkState(t, path)
		})
	}
}
func TestQABindingReplayPrecedesMissingPathAndAccountProvider(t *testing.T) {
	s, path := qaLinkService(t)
	in := qaLinkInput(t)
	in.AccountID = ""
	p := qaNewLinkProvider(t)
	d := qaLinkDeps(t, p)
	d.ResolveAccount = func(context.Context) (string, error) { return "1", nil }
	first, err := s.Link(context.Background(), in, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(in.Path); err != nil {
		t.Fatal(err)
	}
	forbidden := LinkDependencies{ResolveAccount: func(context.Context) (string, error) { t.Fatal("replay resolved account"); return "", nil }, NewProvider: func(context.Context, string) (harvest.Provider, error) {
		t.Fatal("replay constructed provider")
		return nil, nil
	}}
	again, err := New(Options{Path: path}).Link(context.Background(), in, forbidden)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, again) {
		t.Fatalf("replay changed result: %+v %+v", first, again)
	}
	in.AccountID = "9"
	_, err = s.Link(context.Background(), in, forbidden)
	qaCode(t, err, "request_conflict")
}

func TestQABindingTaskChoiceIsAssignedAndNeverGuessed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		tasks    []any
		wantCode string
	}{
		{"sole-active", []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("4")}}, harvest.Object{"is_active": false, "task": harvest.Object{"id": json.Number("5")}}}, ""},
		{"ambiguous", []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("4")}}, harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("5")}}}, "input_required"},
		{"none-active", []any{harvest.Object{"is_active": false, "task": harvest.Object{"id": json.Number("4")}}}, "input_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, path := qaLinkService(t)
			in := qaLinkInput(t)
			in.TaskID = ""
			p := qaNewLinkProvider(t)
			p.assignments[0]["task_assignments"] = tc.tasks
			r, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
			if tc.wantCode != "" {
				qaCode(t, err, tc.wantCode)
				qaAbsentLinkState(t, path)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if r.Binding.Attribution.TaskID != "4" {
				t.Fatalf("wrong sole task %+v", r)
			}
		})
	}
}
func TestQABindingUnknownDurabilityReplayNeedsFreshBarrier(t *testing.T) {
	s, path := qaLegacyLinkService(t)
	in := qaLinkInput(t)
	p := qaNewLinkProvider(t)
	s.store.fail = func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("injected")
		}
		return nil
	}
	_, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	qaCode(t, err, "local_write_unknown")
	var ae *Error
	if !errors.As(err, &ae) || ae.Details["request_id"] != in.RequestID {
		t.Fatalf("unknown result lacks replay identity: %#v", err)
	}
	if err := os.Remove(in.Path); err != nil {
		t.Fatal(err)
	}
	forbidden := LinkDependencies{ResolveAccount: func(context.Context) (string, error) { t.Fatal("replay resolved account"); return "", nil }, NewProvider: func(context.Context, string) (harvest.Provider, error) {
		t.Fatal("replay constructed provider")
		return nil, nil
	}}
	restarted := qaLegacyNew(Options{Path: path})
	restarted.store.fail = s.store.fail
	_, err = restarted.Link(context.Background(), in, forbidden)
	qaCode(t, err, "local_write_unknown")
	restarted.store.fail = nil
	r, err := restarted.Link(context.Background(), in, forbidden)
	if err != nil {
		t.Fatal(err)
	}
	list, err := restarted.ListBindings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Bindings) != 1 || list.Bindings[0].ID != r.Binding.ID || r.Binding.Revision != "1" {
		t.Fatalf("retry duplicated effect %+v %+v", r, list)
	}
	again, err := restarted.Link(context.Background(), in, forbidden)
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatalf("retry changed result %+v %v", again, err)
	}
}
func TestQABindingAttachedActorsBlockMutationButAllowIdenticalLink(t *testing.T) {
	for _, kind := range []string{"work", "wait_user", "wait_permission", "wait_children", "stale"} {
		t.Run(kind, func(t *testing.T) {
			h := qaNew(t)
			h.service = New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr })})
			s := h.service
			in := qaLinkInput(t)
			p := qaNewLinkProvider(t)
			linked, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			snapshot := h.snapshot()
			event := qaEvent("attached", "1", "1", "work", linked.Binding.ID)
			event.Actor.ComputerID = *snapshot.ComputerID
			h.ingest(0, event)
			if kind != "work" {
				event.Sequence = "2"
				event.EventID = "attached-2"
				event.BindingID = ""
				event.BindingRevision = ""
				event.Kind = kind
				if kind == "stale" {
					event.Kind = "finish"
					h.at(10)
					h.clockErr = errors.New("clock unavailable")
					_, err = s.Ingest(context.Background(), event)
					qaCode(t, err, "clock_unavailable")
					h.at(20)
				} else {
					h.ingest(10, event)
				}
			}
			in.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
			in.IfRevision = linked.Binding.Revision
			same, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
			if err != nil || same.Changed || same.Binding.Revision != linked.Binding.Revision {
				t.Fatalf("attached identical link rejected/changed: %+v %v", same, err)
			}
			_, err = s.Unlink(context.Background(), UnlinkInput{BindingID: linked.Binding.ID, IfRevision: linked.Binding.Revision, RequestID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Confirmed: true})
			qaCode(t, err, "binding_in_use")
			_, err = s.RepairBinding(context.Background(), RepairBindingInput{BindingID: linked.Binding.ID, Path: t.TempDir(), IfRevision: linked.Binding.Revision, RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", Confirmed: true})
			qaCode(t, err, "binding_in_use")
			in.Timezone = "America/New_York"
			in.RequestID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
			_, err = s.Link(context.Background(), in, qaLinkDeps(t, p))
			qaCode(t, err, "binding_in_use")
		})
	}
}
func TestQABindingUnlinkTombstoneAndMovedRepairPreserveIdentity(t *testing.T) {
	s, path := qaLinkService(t)
	in := qaLinkInput(t)
	p := qaNewLinkProvider(t)
	first, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	moved := in.Path + "-moved"
	if err := os.Rename(in.Path, moved); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(moved) })
	repair := RepairBindingInput{BindingID: first.Binding.ID, Path: moved, IfRevision: first.Binding.Revision, RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Confirmed: true}
	r, err := s.RepairBinding(context.Background(), repair)
	if err != nil {
		t.Fatal(err)
	}
	if r.Binding.ID != first.Binding.ID || r.Binding.Attribution != first.Binding.Attribution || r.Binding.Revision == first.Binding.Revision {
		t.Fatalf("repair lost identity/history %+v", r)
	}
	again, err := New(Options{Path: path}).RepairBinding(context.Background(), repair)
	if err != nil || !reflect.DeepEqual(r, again) {
		t.Fatalf("repair replay %+v %v", again, err)
	}
	unlink := UnlinkInput{BindingID: r.Binding.ID, IfRevision: r.Binding.Revision, RequestID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Confirmed: true}
	removed, err := s.Unlink(context.Background(), unlink)
	if err != nil || !removed.Changed {
		t.Fatalf("unlink %+v %v", removed, err)
	}
	replay, err := New(Options{Path: path}).Unlink(context.Background(), unlink)
	if err != nil || !reflect.DeepEqual(removed, replay) {
		t.Fatalf("unlink replay %+v %v", replay, err)
	}
	list, err := s.ListBindings(context.Background())
	if err != nil || len(list.Bindings) != 0 {
		t.Fatalf("tombstone live in list %+v %v", list, err)
	}
	_, err = s.ShowBinding(context.Background(), ShowBindingInput{BindingID: r.Binding.ID})
	qaCode(t, err, "not_found")
}
func TestQABindingDirectoryBecomingGitRejectsNewActorButKeepsExistingStop(t *testing.T) {
	h := qaNew(t)
	h.service = New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr })})
	in := qaLinkInput(t)
	r, err := h.service.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil {
		t.Fatal(err)
	}
	computer := *h.snapshot().ComputerID
	e := qaEvent("old", "1", "1", "work", r.Binding.ID)
	e.Actor.ComputerID = computer
	h.ingest(0, e)
	qaGit(t, in.Path, "init")
	fresh := qaEvent("fresh", "1", "1", "work", r.Binding.ID)
	fresh.Actor.ComputerID = computer
	h.at(5)
	_, err = h.service.Ingest(context.Background(), fresh)
	qaCode(t, err, "binding_unavailable")
	e.Sequence = "2"
	e.EventID = "old-finish"
	e.Kind = "finish"
	e.BindingID = ""
	e.BindingRevision = ""
	e.CWD = in.Path
	h.ingest(10, e)
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
}

func TestQABindingIdleCompatibilityAndSavedSelections(t *testing.T) {
	s, _ := qaLinkService(t)
	in := qaLinkInput(t)
	p := qaNewLinkProvider(t)
	first, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	saved := in
	saved.TaskID = ""
	saved.Timezone = ""
	saved.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	p.assignments[0]["task_assignments"] = append(p.assignments[0]["task_assignments"].([]any), harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("5")}})
	r, err := s.Link(context.Background(), saved, qaLinkDeps(t, p))
	if err != nil || r.Changed || r.Binding.Attribution != first.Binding.Attribution {
		t.Fatalf("verified saved selection lost: %+v %v", r, err)
	}
	second := in
	second.Path = t.TempDir()
	second.RequestID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	if _, err := s.Link(context.Background(), second, qaLinkDeps(t, p)); err != nil {
		t.Fatalf("compatible second binding: %v", err)
	}
	for idx, field := range []string{"user_id", "task_id", "timezone"} {
		bad := second
		bad.Path = t.TempDir()
		bad.RequestID = fmt.Sprintf("dddddddd-dddd-4ddd-8ddd-%012d", idx)
		other := qaNewLinkProvider(t)
		switch field {
		case "user_id":
			other.user["id"] = json.Number("99")
		case "task_id":
			bad.TaskID = "5"
			other.assignments[0]["task_assignments"] = []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("5")}}}
		case "timezone":
			bad.Timezone = "America/New_York"
		}
		_, err := s.Link(context.Background(), bad, qaLinkDeps(t, other))
		qaCode(t, err, "attribution_conflict")
		var ae *Error
		errors.As(err, &ae)
		raw, _ := json.Marshal(ae.Details)
		if !strings.Contains(string(raw), field) {
			t.Fatalf("missing safe differing field %s: %s", field, raw)
		}
	}
}
func TestQABindingRevisionAndConfirmationGuards(t *testing.T) {
	s, _ := qaLinkService(t)
	in := qaLinkInput(t)
	p := qaNewLinkProvider(t)
	first, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Unlink(context.Background(), UnlinkInput{BindingID: first.Binding.ID, IfRevision: first.Binding.Revision, RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"})
	qaCode(t, err, "confirmation_required")
	_, err = s.Unlink(context.Background(), UnlinkInput{BindingID: first.Binding.ID, IfRevision: "99", RequestID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Confirmed: true})
	qaCode(t, err, "revision_conflict")
	in.Timezone = "America/New_York"
	in.RequestID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	_, err = s.Link(context.Background(), in, qaLinkDeps(t, p))
	qaCode(t, err, "revision_conflict")
	in.IfRevision = first.Binding.Revision
	r, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil || !r.Changed || r.Binding.ID != first.Binding.ID || r.Binding.Revision == first.Binding.Revision {
		t.Fatalf("valid revision update %+v %v", r, err)
	}
}
func TestQABindingProcessHelper(t *testing.T) {
	if os.Getenv("TEMPO_QA_BINDING_HELPER") != "1" {
		return
	}
	s := qaLegacyNew(Options{Path: os.Getenv("TEMPO_QA_BINDING_STATE")})
	in := LinkInput{Path: os.Getenv("TEMPO_QA_BINDING_PATH"), ProjectID: "3", TaskID: "4", AccountID: "1", Timezone: "UTC", RequestID: os.Getenv("TEMPO_QA_BINDING_REQUEST")}
	if _, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t))); err != nil {
		t.Fatal(err)
	}
}
func TestQABindingConcurrentProcessesProduceOneLiveLocator(t *testing.T) {
	for _, sameRequest := range []bool{false, true} {
		t.Run(fmt.Sprintf("same-request-%t", sameRequest), func(t *testing.T) {
			s, path := qaLegacyLinkService(t)
			location := t.TempDir()
			var cmds []*exec.Cmd
			type outcome struct {
				output []byte
				err    error
			}
			done := make(chan outcome, 4)
			for i := 0; i < 4; i++ {
				request := fmt.Sprintf("aaaaaaaa-aaaa-4aaa-8aaa-%012d", i)
				if sameRequest {
					request = qaLinkRequest
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestQABindingProcessHelper$")
				cmd.Env = append(os.Environ(), "TEMPO_QA_BINDING_HELPER=1", "TEMPO_QA_BINDING_STATE="+path, "TEMPO_QA_BINDING_PATH="+location, "TEMPO_QA_BINDING_REQUEST="+request)
				cmds = append(cmds, cmd)
			}
			for _, cmd := range cmds {
				go func(c *exec.Cmd) { out, err := c.CombinedOutput(); done <- outcome{out, err} }(cmd)
			}
			var failures []string
			for range cmds {
				r := <-done
				if r.err != nil {
					failures = append(failures, fmt.Sprintf("%v %s", r.err, r.output))
				}
			}
			if len(failures) > 0 {
				t.Fatalf("concurrent Link failures: %v", failures)
			}
			list, err := s.ListBindings(context.Background())
			if err != nil || len(list.Bindings) != 1 || list.Bindings[0].Revision != "1" {
				t.Fatalf("concurrent duplicate effects: %+v %v", list, err)
			}
			st, _, err := s.store.read(context.Background())
			expectedReceipts := 4
			if sameRequest {
				expectedReceipts = 1
			}
			if err != nil || !validUUID(st.ComputerID) || len(st.Requests) != expectedReceipts {
				t.Fatalf("lost durable identity/receipt: state=%+v error=%v", st, err)
			}
		})
	}
}

func TestQABindingCorruptRecordsAndReceiptsPreserveEvidence(t *testing.T) {
	for _, name := range []string{"receipt-fingerprint", "receipt-operation", "receipt-result-request", "receipt-missing-result", "record-kind", "record-relative-locator", "record-snapshot-mismatch"} {
		t.Run(name, func(t *testing.T) {
			s, path := qaLegacyLinkService(t)
			in := qaLinkInput(t)
			first, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var data map[string]any
			if err := json.Unmarshal(raw, &data); err != nil {
				t.Fatal(err)
			}
			receipt := data["requests"].(map[string]any)[in.RequestID].(map[string]any)
			record := data["binding_records"].(map[string]any)[first.Binding.ID].(map[string]any)
			switch name {
			case "receipt-fingerprint":
				receipt["fingerprint"] = "bad"
			case "receipt-operation":
				receipt["operation"] = "links.unlink"
			case "receipt-result-request":
				receipt["binding_result"].(map[string]any)["request_id"] = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
			case "receipt-missing-result":
				delete(receipt, "binding_result")
			case "record-kind":
				record["kind"] = "arbitrary"
			case "record-relative-locator":
				record["locator"] = "../relative"
			case "record-snapshot-mismatch":
				record["snapshot"].(map[string]any)["revision"] = "99"
			}
			bad, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, bad, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = qaLegacyNew(Options{Path: path}).ListBindings(context.Background())
			qaCode(t, err, "state_corrupt")
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(bad) {
				t.Fatalf("corrupt evidence changed: %v", err)
			}
		})
	}
}

func TestQABindingCommitRechecksAttachmentAfterRemotePreflight(t *testing.T) {
	h := qaNew(t)
	h.service = New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr })})
	s := h.service
	in := qaLinkInput(t)
	first, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil {
		t.Fatal(err)
	}
	computer := *h.snapshot().ComputerID
	changed := in
	changed.IfRevision = first.Binding.Revision
	changed.Timezone = "America/New_York"
	changed.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	p := qaNewLinkProvider(t)
	p.beforeList = func() {
		e := qaEvent("late-attachment", "1", "1", "work", first.Binding.ID)
		e.Actor.ComputerID = computer
		r := h.ingest(1, e)
		if r.Disposition != "applied" {
			t.Fatalf("late actor not registered: %+v", r)
		}
	}
	_, err = s.Link(context.Background(), changed, qaLinkDeps(t, p))
	qaCode(t, err, "binding_in_use")
	current, err := s.ShowBinding(context.Background(), ShowBindingInput{BindingID: first.Binding.ID})
	if err != nil || current.Bindings[0].Revision != first.Binding.Revision || current.Bindings[0].Attribution != first.Binding.Attribution {
		t.Fatalf("concurrent attachment was overwritten: %+v %v", current, err)
	}
}
func TestQABindingRelinkPreservesFinalizedAttribution(t *testing.T) {
	h := qaNew(t)
	h.service = qaLegacyNew(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr })})
	in := qaLinkInput(t)
	p := qaNewLinkProvider(t)
	first, err := h.service.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	computer := *h.snapshot().ComputerID
	event := qaEvent("history", "1", "1", "work", first.Binding.ID)
	event.Actor.ComputerID = computer
	h.ingest(0, event)
	event.Kind = "finish"
	event.Sequence = "2"
	event.EventID = "history-finish"
	event.BindingID = ""
	event.BindingRevision = ""
	h.ingest(10, event)
	before := h.snapshot()
	qaIntervals(t, before, [][2]int64{{0, 10}})
	st, _, err := h.service.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	outboxBefore, _ := json.Marshal(st.Outbox)
	in.IfRevision = first.Binding.Revision
	in.Timezone = "America/New_York"
	in.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	if _, err := h.service.Link(context.Background(), in, qaLinkDeps(t, p)); err != nil {
		t.Fatal(err)
	}
	after := h.snapshot()
	if !reflect.DeepEqual(before.ClosedIntervals, after.ClosedIntervals) || !reflect.DeepEqual(before.Actors, after.Actors) {
		t.Fatalf("relink rewrote historical attribution: before=%+v after=%+v", before, after)
	}
	st, _, err = h.service.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	outboxAfter, _ := json.Marshal(st.Outbox)
	if string(outboxBefore) != string(outboxAfter) {
		t.Fatal("relink rewrote durable outbox")
	}
}

func TestQABindingRepairRejectsDestinationCollisionAndScopeChange(t *testing.T) {
	s, _ := qaLinkService(t)
	p := qaNewLinkProvider(t)
	in := qaLinkInput(t)
	first, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	other := qaLinkInput(t)
	other.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	if _, err := s.Link(context.Background(), other, qaLinkDeps(t, p)); err != nil {
		t.Fatal(err)
	}
	_, err = s.RepairBinding(context.Background(), RepairBindingInput{BindingID: first.Binding.ID, Path: other.Path, IfRevision: first.Binding.Revision, RequestID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Confirmed: true})
	if err == nil {
		t.Fatal("repair silently collided with other live binding")
	}
	repo := t.TempDir()
	qaGit(t, repo, "init")
	_, err = s.RepairBinding(context.Background(), RepairBindingInput{BindingID: first.Binding.ID, Path: repo, IfRevision: first.Binding.Revision, RequestID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", Confirmed: true})
	if err == nil {
		t.Fatal("repair silently changed directory into repository scope")
	}
	current, err := s.ShowBinding(context.Background(), ShowBindingInput{BindingID: first.Binding.ID})
	if err != nil || !reflect.DeepEqual(current.Bindings[0], first.Binding) {
		t.Fatalf("failed repair mutated binding: %+v %v", current, err)
	}
}

func TestQABindingUnlinkedRealChildLocationInheritsParent(t *testing.T) {
	h := qaNew(t)
	h.service = New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr })})
	in := qaLinkInput(t)
	linked, err := h.service.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil {
		t.Fatal(err)
	}
	computer := *h.snapshot().ComputerID
	parent := qaEvent("real-parent", "1", "1", "work", linked.Binding.ID)
	parent.Actor.ComputerID = computer
	h.ingest(0, parent)
	child := qaEvent("real-child", "1", "1", "work", "")
	child.Actor.ComputerID = computer
	child.CWD = t.TempDir()
	child.Parent = &ActorRef{Key: parent.Actor, Generation: "1"}
	result := h.ingest(5, child)
	if result.Disposition != "applied" {
		t.Fatalf("unlinked child did not inherit: %+v", result)
	}
	snapshot := h.snapshot()
	if len(snapshot.Actors) != 2 {
		t.Fatalf("parent/child absent: %+v", snapshot.Actors)
	}
	for _, actor := range snapshot.Actors {
		if actor.BindingID != linked.Binding.ID || actor.Attribution != linked.Binding.Attribution {
			t.Fatalf("child attributed independently: %+v", actor)
		}
	}
}

func TestQABindingCommitRechecksRevisionAfterRemotePreflight(t *testing.T) {
	s, _ := qaLinkService(t)
	in := qaLinkInput(t)
	first, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil {
		t.Fatal(err)
	}
	changed := in
	changed.IfRevision = first.Binding.Revision
	changed.Timezone = "America/New_York"
	changed.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	p := qaNewLinkProvider(t)
	p.beforeList = func() {
		other := changed
		other.Timezone = "Europe/London"
		other.RequestID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
		if _, err := s.Link(context.Background(), other, qaLinkDeps(t, qaNewLinkProvider(t))); err != nil {
			t.Fatal(err)
		}
	}
	_, err = s.Link(context.Background(), changed, qaLinkDeps(t, p))
	qaCode(t, err, "revision_conflict")
	current, err := s.ShowBinding(context.Background(), ShowBindingInput{BindingID: first.Binding.ID})
	if err != nil || current.Bindings[0].Attribution.Timezone != "Europe/London" {
		t.Fatalf("concurrent revision overwritten: %+v %v", current, err)
	}
}
func TestQABindingDirectoryAncestorBecomingGitRejectsIDOnlyIngress(t *testing.T) {
	h := qaNew(t)
	h.service = New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr })})
	ancestor := t.TempDir()
	child := filepath.Join(ancestor, "child")
	if err := os.Mkdir(child, 0700); err != nil {
		t.Fatal(err)
	}
	in := qaLinkInput(t)
	in.Path = child
	linked, err := h.service.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil {
		t.Fatal(err)
	}
	computer := *h.snapshot().ComputerID
	qaGit(t, ancestor, "init")
	event := qaEvent("ancestor-boundary", "1", "1", "work", linked.Binding.ID)
	event.Actor.ComputerID = computer
	_, err = h.service.Ingest(context.Background(), event)
	qaCode(t, err, "binding_unavailable")
	if len(h.snapshot().Actors) != 0 {
		t.Fatal("new repository scope admitted inherited directory actor")
	}
}
