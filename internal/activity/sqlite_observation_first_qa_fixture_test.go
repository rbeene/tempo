//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
)

type obQAFixture struct {
	t                          *testing.T
	ctx                        context.Context
	path, policyPath, computer string
	cwd                        []string
	bindings                   []Binding
	policies                   *hookstate.Service
	sample                     ClockSample
	clockErr                   error
	calls                      int
	forbid                     bool
}

func obQAID(n int) string { return fmt.Sprintf("ec000000-0000-4000-8000-%012d", n) }
func (q *obQAFixture) when(seconds int64) time.Time {
	return time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC).Add(time.Duration(seconds) * time.Second)
}
func (q *obQAFixture) at(seconds int64) {
	epoch, n := "observation-boot", strconv.FormatInt(seconds*int64(time.Second), 10)
	q.sample = ClockSample{Capability: "available", WallUTC: q.when(seconds), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}
	q.clockErr = nil
}

// Only public Link, Ingest and basic supported IngestHost initialize activity.
// No state object, SQL seed or private mutation helper is used by this fixture.
func obQANew(t *testing.T, count int, host bool) *obQAFixture {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "tempo-observation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error("owned fixture cleanup", err)
		}
	})
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	q := &obQAFixture{t: t, ctx: ctx, path: filepath.Join(canonical, "activity", "state.json"), policyPath: filepath.Join(canonical, "policy", "hooks.json"), cwd: []string{}, bindings: []Binding{}}
	q.policies = hookstate.New(hookstate.Options{Path: q.policyPath})
	q.at(0)
	for i := 0; i < count; i++ {
		cwd := filepath.Join(canonical, fmt.Sprintf("project-%d", i))
		if err := os.Mkdir(cwd, 0700); err != nil {
			t.Fatal(err)
		}
		q.cwd = append(q.cwd, cwd)
		project := strconv.Itoa(3 + i*2)
		p := qaNewLinkProvider(t)
		p.assignments[0]["project"] = harvest.Object{"id": json.Number(project)}
		in := LinkInput{ProjectID: project, TaskID: "4", AccountID: "1", Timezone: "UTC", Path: cwd, RequestID: obQAID(i + 1)}
		result, err := q.service().Link(ctx, in, qaLinkDeps(t, p))
		if err != nil || !result.Changed || result.Binding.ID == "" || result.Binding.Locator != cwd || result.Binding.Attribution != (Attribution{AccountID: "1", UserID: "2", ProjectID: project, TaskID: "4", Timezone: "UTC"}) {
			t.Fatal("public Link prerequisite", err)
		}
		q.bindings = append(q.bindings, result.Binding)
	}
	if count > 0 {
		snapshot := q.snapshot()
		if snapshot.ComputerID == nil {
			t.Fatal("public Link did not initialize identity")
		}
		q.computer = *snapshot.ComputerID
	}
	if host {
		if count < 1 {
			t.Fatal("host fixture requires a public binding")
		}
		c := hookstate.Context{Host: "codex", Scope: "project", Path: q.cwd[0], RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}}
		for _, role := range []string{"runtime", "executable", "definitions"} {
			path := filepath.Join(q.cwd[0], role)
			if err := os.WriteFile(path, []byte("synthetic observation "+role), 0600); err != nil {
				t.Fatal(err)
			}
			c.Artifacts = append(c.Artifacts, hookstate.Artifact{Role: role, Path: path})
		}
		p, err := q.policies.Preview(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		r, err := q.policies.Confirm(ctx, hookstate.ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: obQAID(9), Confirmed: true})
		if err != nil || !r.CaptureEligible || r.Basis != "operator_declared" {
			t.Fatal("public declared host policy prerequisite", err)
		}
	}
	return q
}

func (q *obQAFixture) service() *Service {
	return NewSQLite(Options{Path: q.path, LockTimeout: sqliteFlowTestLockTimeout(), HookPolicies: q.policies, Clock: ClockFunc(func() (ClockSample, error) {
		q.calls++
		if q.forbid {
			q.t.Error("observation/read replay sampled a clock")
			return ClockSample{}, errors.New("forbidden sample")
		}
		return q.sample, q.clockErr
	})})
}

func (q *obQAFixture) quiet(fn func()) {
	q.t.Helper()
	before, old := q.calls, q.forbid
	q.forbid = true
	defer func() {
		q.forbid = old
		if q.calls != before {
			q.t.Error("no-clock path sampled")
		}
	}()
	fn()
}

func (q *obQAFixture) snapshot() ActivitySnapshot {
	q.t.Helper()
	r, err := q.service().Status(q.ctx)
	if err != nil {
		q.t.Fatal("cold public Status prerequisite", err)
	}
	if _, err := os.Lstat(q.path); !errors.Is(err, os.ErrNotExist) {
		q.t.Fatal("private SQL route created/adopted JSON authority", err)
	}
	return r
}

func (q *obQAFixture) event(at int64, binding int, agent, generation, sequence, kind string) EventResult {
	q.t.Helper()
	q.at(at)
	e := Event{ContractVersion: 1, Actor: ActorKey{ComputerID: q.computer, Source: "manual-test", SessionID: "observation", AgentID: agent}, Generation: generation, Sequence: sequence, EventID: "observation/" + agent + "/" + generation + "/" + sequence, Kind: kind}
	if sequence == "1" {
		e.BindingID = q.bindings[binding].ID
		e.BindingRevision = q.bindings[binding].Revision
		e.CWD = q.cwd[binding]
	}
	r, err := q.service().Ingest(q.ctx, e)
	if err != nil || r.Disposition != "applied" || r.Actor != (ActorRef{Key: e.Actor, Generation: generation}) {
		q.t.Fatal("public normalized setup prerequisite", err)
	}
	return r
}

func (q *obQAFixture) host(at int64, session, kind, turn, agent string) HostReceipt {
	q.t.Helper()
	q.at(at)
	e := HostEvent{Source: "codex", SessionID: session, Kind: kind, TurnID: turn, AgentID: agent, CWD: q.cwd[0]}
	if kind == "SessionStart" {
		e.SessionSource = "startup"
	}
	r, err := q.service().IngestHost(q.ctx, e)
	if err != nil || r.ID == "" || r.Durability != "committed" || r.Origin != "unverified" || r.ProfileBasis != "operator_declared" || r.DiagnosticCode != "" {
		q.t.Fatal("basic public host setup prerequisite", err)
	}
	return r
}

func obQAActor(t *testing.T, s ActivitySnapshot, ref ActorRef) Actor {
	t.Helper()
	for _, a := range s.Actors {
		if a.Ref == ref {
			return a
		}
	}
	t.Fatal("expected exact actor absent")
	return Actor{}
}
func obQAUncertainty(t *testing.T, s ActivitySnapshot, ref ActorRef) Uncertainty {
	t.Helper()
	var matches []Uncertainty
	for _, u := range s.Uncertainties {
		if u.Actor == ref {
			matches = append(matches, u)
		}
	}
	if len(matches) != 1 {
		t.Fatal("expected one exact-generation uncertainty")
	}
	return matches[0]
}
func obQAError(t *testing.T, err error, code string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("observation error code: got %v, want %s", err, code)
	}
}
func obQAMutation(t *testing.T, r MutationResult, id string, changed bool, ids []string, before, after string) {
	t.Helper()
	sort.Strings(ids)
	n, err := strconv.ParseUint(before, 10, 64)
	if err != nil || r.ContractVersion != 1 || r.RequestID != id || r.Changed != changed || r.EntityRevision != nil || r.AffectedIDs == nil || !reflect.DeepEqual(r.AffectedIDs, ids) || r.SnapshotRevision != after || after != strconv.FormatUint(n+1, 10) {
		t.Fatal("typed observation receipt/revision/affected IDs")
	}
}
func obQASameFacts(t *testing.T, before, after ActivitySnapshot) {
	t.Helper()
	before.SnapshotRevision = after.SnapshotRevision
	if !reflect.DeepEqual(before, after) {
		t.Fatal("no-op receipt changed public domain facts")
	}
}
