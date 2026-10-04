//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Source-only status fixtures. The private Link, Ingest and Status ports are
// composed by the coordinator; this file does not activate public dispatch.
import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	_ "unsafe"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type stQAFixture struct {
	t        *testing.T
	f        interopFixture
	service  *Service
	sample   ClockSample
	samples  int
	computer string
	bindings map[string]BindingSnapshot
	first    BindingResult
	input    LinkInput
	provider *qaLinkProvider
}

func stQAAt(seconds int64) ClockSample {
	epoch := "boot-1"
	n := durationString(time.Duration(seconds) * time.Second)
	return ClockSample{Capability: "available", WallUTC: qaEpochStart.Add(time.Duration(seconds) * time.Second), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}
}
func stQALinked(t *testing.T) *stQAFixture {
	t.Helper()
	q := &stQAFixture{t: t, f: interopLocation(t), sample: stQAAt(0), bindings: map[string]BindingSnapshot{}}
	q.service = New(Options{Path: filepath.Join(q.f.directory, q.f.authority), Clock: ClockFunc(func() (ClockSample, error) { q.samples++; return q.sample, nil }), ResolveBinding: func(_ context.Context, e Event) (BindingSnapshot, bool, error) {
		b, ok := q.bindings[e.BindingID]
		return b, ok, nil
	}})
	q.input = qaLinkInput(t)
	q.provider = qaNewLinkProvider(t)
	var err error
	q.first, err = q.service.linkSQLite(context.Background(), q.input, qaLinkDeps(t, q.provider))
	if err != nil || !q.first.Changed {
		t.Fatal("real fresh link prerequisite", err)
	}
	q.bindings[q.first.Binding.ID] = BindingSnapshot{ID: q.first.Binding.ID, Revision: q.first.Binding.Revision, Attribution: q.first.Binding.Attribution}
	meta, _ := stQAAudit(t, q.f)
	q.computer = meta.ComputerID
	if q.computer == "" || !validUUID(q.computer) {
		t.Fatal("fresh link omitted computer identity")
	}
	return q
}
func (q *stQAFixture) at(seconds int64) { q.sample = stQAAt(seconds) }
func (q *stQAFixture) event(actor, sequence, kind string, b BindingSnapshot) Event {
	e := qaEvent(actor, "1", sequence, kind, "")
	e.Actor.ComputerID = q.computer
	if kind == "work" && b.ID != "" {
		e.BindingID = b.ID
		e.BindingRevision = b.Revision
	}
	return e
}
func (q *stQAFixture) ingest(seconds int64, e Event) EventResult {
	q.t.Helper()
	q.at(seconds)
	r, err := q.service.ingestSQLite(context.Background(), e)
	if err != nil {
		q.t.Fatalf("real normalized capture %s/%s: %v", e.Kind, e.Sequence, err)
	}
	return r
}
func (q *stQAFixture) status() ActivitySnapshot {
	q.t.Helper()
	got, err := q.service.statusSQLite(context.Background())
	if err != nil {
		q.t.Fatal("status", err)
	}
	return got
}

type stQAOwner struct {
	t                                  *testing.T
	c                                  *sqliteio.Conn
	tx                                 *sqliteio.Tx
	ended, closed, reported, exhausted bool
	closeAttempts                      int
	terminalErr                        error
}

func stQAOpen(t *testing.T, f interopFixture, mode sqliteio.Mode) *stQAOwner {
	t.Helper()
	c, err := sqliteio.Open(context.Background(), f.directory, f.database, sqliteio.Options{ReadOnly: mode == sqliteio.Read, AcquireDeadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal("cold native open", err)
	}
	owner := &stQAOwner{t: t, c: c}
	t.Cleanup(owner.cleanup) // One owner is registered before Begin; tx joins it below.
	tx, err := c.Begin(context.Background(), mode)
	if err != nil {
		cleanup := owner.finish(false)
		owner.reported = true
		t.Fatal("cold native begin/close", errors.Join(err, cleanup))
	}
	owner.tx = tx
	return owner
}

// finish gives an accepted Commit exactly one dispatch. NotAttempted leaves a
// live Tx and must be rolled back by the same owner before checking Close.
// CloseChecked's terminal bit, rather than a nil error, releases ownership.
func (owner *stQAOwner) finish(commit bool) error {
	if owner == nil {
		return nil
	}
	if owner.tx != nil && !owner.ended {
		if commit {
			outcome, err := owner.tx.Commit()
			owner.terminalErr = errors.Join(owner.terminalErr, err)
			if outcome == sqliteio.NotAttempted {
				owner.terminalErr = errors.Join(owner.terminalErr, owner.tx.Rollback())
			}
			if outcome != sqliteio.Committed {
				owner.terminalErr = errors.Join(owner.terminalErr, errors.New("fixture commit not confirmed"))
			}
		} else {
			owner.terminalErr = errors.Join(owner.terminalErr, owner.tx.Rollback())
		}
		owner.ended = true
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for owner.c != nil && !owner.closed && owner.closeAttempts < 2 {
		owner.closeAttempts++
		terminal, err := owner.c.CloseChecked(closeCtx)
		owner.terminalErr = errors.Join(owner.terminalErr, err)
		owner.closed = terminal
	}
	if owner.c != nil && !owner.closed && owner.closeAttempts == 2 && !owner.exhausted {
		owner.exhausted = true
		owner.terminalErr = errors.Join(owner.terminalErr, errors.New("fixture connection ownership remains nonterminal"))
	}
	return owner.terminalErr
}
func (owner *stQAOwner) cleanup() {
	err := owner.finish(false)
	if err != nil && !owner.reported {
		owner.reported = true
		owner.t.Errorf("owned fixture native cleanup: %v", err)
	}
}
func stQAClose(t *testing.T, owner *stQAOwner, commit bool) {
	t.Helper()
	if err := owner.finish(commit); err != nil {
		owner.reported = true
		t.Fatal("fixture terminal action/checked close", err)
	}
}
func stQAAudit(t *testing.T, f interopFixture) (sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	owner := stQAOpen(t, f, sqliteio.Read)
	defer owner.cleanup()
	meta, err := sqliteReadMeta(owner.tx, f.authority, f.database)
	if err != nil {
		t.Fatal(err)
	}
	rows, charge := cfQAAudit(t, owner.tx, meta)
	if charge != meta.LogicalBytes {
		t.Fatal("cold literal charge differs from metadata")
	}
	cfQAColdClosure(t, owner.tx, meta, true)
	stQAClose(t, owner, false)
	return meta, rows
}
func stQAUnchanged(t *testing.T, f interopFixture, before sqliteStoreMeta, rows map[string][][]string) {
	t.Helper()
	after, got := stQAAudit(t, f)
	if !reflect.DeepEqual(after, before) || !reflect.DeepEqual(got, rows) {
		t.Fatal("status changed durable rows, revision, charge or nonce")
	}
}

// Exact target sqliteio private SQL observation ABI. These hooks only inject
// checked read cleanup failures and prove native release ordering.
type stQASQLEvent struct {
	Phase, Operation string
	Code             int32
}
type stQASQLHooks struct {
	Observe func(stQASQLEvent)
	Fault   func(stQASQLEvent) error
}

//go:linkname stQASetSQLHooks github.com/rbeene/tempo/internal/activity/sqliteio.setSQLHooksForTest
func stQASetSQLHooks(stQASQLHooks)
