//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Test-first private host route. Native-shaped metadata is not proof of an
// installed host emitting callbacks; real decoder/CLI delivery is a later gate.
import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"golang.org/x/sys/unix"
)

func TestSQLiteHostIngressH01RootChildAndIndependentProjectUnions(t *testing.T) {
	// Only this functional flow opts into the instrumented-build budget. Keep
	// the fixture's clock and policy objects; ordinary builds still select 0.
	useFlowBudget := func(h *hiQAFixture) {
		h.s = New(Options{Path: h.path, LockTimeout: sqliteFlowTestLockTimeout(), HookPolicies: h.policies, Clock: h.s.clock})
	}
	t.Run("root-child-reopen-replay", func(t *testing.T) {
		h := hiQANew(t, "codex", 1)
		useFlowBudget(h)
		start := h.event("SessionStart", "", "")
		start.SessionSource = "startup"
		events := []HostEvent{start, h.event("UserPromptSubmit", "root-turn", ""), h.event("SubagentStart", "child-turn", "child"), h.event("Stop", "root-turn", ""), h.event("SubagentStop", "child-turn", "child")}
		times := []int64{0, 0, 10, 20, 30}
		accepted := []HostReceipt{}
		for n, e := range events {
			accepted = append(accepted, h.send(times[n], e))
			if accepted[n].SnapshotRevision != strconv.Itoa(n+2) {
				t.Fatal("host callback minted more than one public revision", accepted[n])
			}
			if n == 3 {
				if h.actor(accepted[1].Actor).State != "wait_user" || h.actor(accepted[2].Actor).State != "working" {
					t.Fatal("root Stop terminated child or finished root")
				}
				h.intervals("3")
			}
		}
		h.intervals("3", [2]int64{0, 30})
		before := h.snapshot()
		if before.Meta.ComputerID != interopComputer || before.Meta.Revision != "6" || before.Meta.SyncEnabled {
			t.Fatal("host capture changed identity, revision policy or sync consent")
		}
		if len(before.Rows["host_receipts"]) != 5 || len(before.Rows["event_receipts"]) != 4 || len(before.Rows["actors"]) != 2 || len(before.Rows["actor_generations"]) != 2 || len(before.Rows["outbox"]) != 1 {
			t.Fatal("host/normalized receipt or union allocation count", before.Rows)
		}
		h.reopen()
		useFlowBudget(h)
		h.revoke()
		h.clockErr = errors.New("exact historical replay must not sample this clock")
		for n, e := range events {
			calls := h.clockCalls
			r, err := h.s.ingestHostSQLite(context.Background(), e)
			if err != nil || r.Disposition != "duplicate" {
				t.Fatal("historical exact host replay", err, r)
			}
			want := accepted[n]
			want.Disposition = "duplicate"
			if !reflect.DeepEqual(r, want) || h.clockCalls != calls {
				t.Fatal("replay changed receipt/reference/revision or used live clock")
			}
			after := h.snapshot()
			hiQANonceOnly(t, before, after)
			before = after
		}
	})
	t.Run("independent-projects", func(t *testing.T) {
		h := hiQANew(t, "codex", 2)
		useFlowBudget(h)
		h.start()
		q := h.event("SessionStart", "", "")
		q.SessionID, q.CWD, q.SessionSource = "independent-Q", h.cwd[1], "startup"
		h.send(0, q)
		h.send(0, h.event("UserPromptSubmit", "P", ""))
		q.Kind, q.TurnID, q.SessionSource = "UserPromptSubmit", "Q", ""
		h.send(0, q)
		h.send(10, h.event("SubagentStart", "P-child", "child"))
		h.send(20, h.event("Stop", "P", ""))
		h.send(30, h.event("SubagentStop", "P-child", "child"))
		q.Kind = "Stop"
		h.send(30, q)
		h.intervals("3", [2]int64{0, 30})
		h.intervals("4", [2]int64{0, 30})
		rows := h.snapshot().Rows
		if len(rows["outbox"]) != 2 || len(rows["intervals"]) != 2 {
			t.Fatal("independent project unions collapsed or double billed")
		}
	})
}

func TestSQLiteHostIngressH02AbsentUnlinkedAndDeniedDoNotAdmit(t *testing.T) {
	for _, mode := range []string{"absent", "unlinked", "denied"} {
		t.Run(mode, func(t *testing.T) {
			bindings := map[string]int{"absent": -1, "unlinked": 0, "denied": 1}[mode]
			h := hiQANew(t, "codex", bindings)
			if mode == "denied" {
				h.revoke()
			}
			var before hiQASnapshot
			if mode != "absent" {
				before = h.snapshot()
			}
			e := h.event("SessionStart", "", "")
			e.SessionSource = "startup"
			r, err := h.s.ingestHostSQLite(context.Background(), e)
			want := "untracked"
			if mode == "denied" {
				want = "review_required"
			}
			if err != nil || r.Disposition != want || r.Actor != nil || r.Durability != "not_committed" || r.Origin != "unverified" {
				t.Fatal("nonadmitted callback fabricated committed capture", r, err)
			}
			if _, err := os.Lstat(h.path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("host ingress created JSON authority")
			}
			if _, err := os.Lstat(h.path + ".lock"); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("host ingress created initialization guard")
			}
			if mode == "absent" {
				entries, err := os.ReadDir(h.f.directory)
				if err != nil || len(entries) != 0 {
					t.Fatal("absent host callback initialized SQLite/guard", err)
				}
				if _, err := os.Lstat(filepath.Dir(h.policyPath)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("absent callback initialized hook policy files", err)
				}
			} else if !reflect.DeepEqual(before, h.snapshot()) {
				t.Fatal("denied/unlinked admission mutated native store")
			}
		})
	}
}

func TestSQLiteHostIngressH03PrecommitFailureRollsBackBothReceipts(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	h.start()
	h.calibrateHook()
	before := h.snapshot()
	reached := false
	spQAHooks(t, spQASQLHooks{Fault: func(e spQASQLEvent) error {
		if e.Phase == "commit-before-dispatch" && e.Operation == "commit" {
			reached = true
			return unix.EIO
		}
		return nil
	}})
	r, err := h.s.ingestHostSQLite(context.Background(), h.event("UserPromptSubmit", "atomic", ""))
	spQASetSQLHooks(spQASQLHooks{})
	var ae *Error
	if !reached || err == nil || r.Durability != "not_committed" || errors.As(err, &ae) && ae.Uncertain {
		t.Fatal("precommit failure falsely acknowledged", r, err)
	}
	if !reflect.DeepEqual(before, h.snapshot()) {
		t.Fatal("host/normalized receipts or actor survived failed COMMIT")
	}
}

func TestSQLiteHostIngressH04CommittedUnknownReceiptFencesExactReplay(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	h.start()
	h.calibrateHook()
	reached, committed := false, false
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Phase == "commit-after-engine" && e.Operation == "commit" && e.Code == 101 {
			committed = true
		}
	}, Fault: func(e spQASQLEvent) error {
		if e.Phase == "durable-native-closed" && e.Operation == "close-durably" {
			reached = true
			return unix.EIO
		}
		return nil
	}})
	e := h.event("UserPromptSubmit", "unknown", "")
	r, err := h.s.ingestHostSQLite(context.Background(), e)
	spQASetSQLHooks(spQASQLHooks{})
	var ae *Error
	if !reached || !committed || !errors.As(err, &ae) || !ae.Uncertain || ae.Code != "local_write_unknown" || r.Durability != "unknown" {
		t.Fatal("actual postcommit barrier failure lost uncertainty", r, err)
	}
	before := h.snapshot()
	if len(before.Rows["actors"]) != 1 || len(before.Rows["event_receipts"]) != 1 || len(before.Rows["host_receipts"]) != 2 {
		t.Fatal("actual COMMIT split host/normalized capture")
	}
	h.reopen()
	h.revoke()
	h.clockErr = errors.New("unknown exact replay cannot depend on clock")
	calls := h.clockCalls
	got, err := h.s.ingestHostSQLite(context.Background(), e)
	if err != nil || got.Disposition != "duplicate" || got.Durability != "committed" || got.ID != r.ID || got.SnapshotRevision != r.SnapshotRevision || !reflect.DeepEqual(got.Actor, r.Actor) || calls != h.clockCalls {
		t.Fatal("unknown retry reapplied or failed exact fencing", got, err)
	}
	hiQANonceOnly(t, before, h.snapshot())
}

func TestSQLiteHostIngressH05WinnerDuringPreparationAllocatesOnce(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	h.start()
	e := h.event("UserPromptSubmit", "winner", "")
	other := New(Options{Path: h.path, HookPolicies: h.policies, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, nil })})
	var winner HostReceipt
	fired := false
	h.s.clock = ClockFunc(func() (ClockSample, error) {
		if !fired {
			fired = true
			var err error
			winner, err = other.ingestHostSQLite(context.Background(), e)
			if err != nil {
				return ClockSample{}, err
			}
		}
		return h.sample, nil
	})
	r, err := h.s.ingestHostSQLite(context.Background(), e)
	if !fired || err != nil || r.Disposition != "duplicate" || r.ID != winner.ID || !reflect.DeepEqual(r.Actor, winner.Actor) {
		t.Fatal("real concurrent preparation winner was not replayed", r, err)
	}
	snapshot := h.snapshot()
	rows := snapshot.Rows
	if snapshot.Meta.Revision != "3" {
		t.Fatal("concurrent loser minted another public revision")
	}
	if len(rows["actors"]) != 1 || len(rows["actor_generations"]) != 1 || len(rows["event_receipts"]) != 1 || len(rows["host_receipts"]) != 2 {
		t.Fatal("concurrent duplicate allocated multiple effects")
	}
}

func TestSQLiteHostIngressH06SafetyOnlyPromptReservesGeneration(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	h.start()
	first := h.send(0, h.event("UserPromptSubmit", "A", ""))
	if first.Actor == nil || first.Actor.Generation != "1" {
		t.Fatal("prompt A did not establish generation 1", first)
	}
	h.at(10)
	h.clockErr = errors.New("unavailable during prompt B admission")
	r, err := h.s.ingestHostSQLite(context.Background(), h.event("UserPromptSubmit", "B", ""))
	qaCode(t, err, "clock_unavailable")
	if r.Durability != "committed" || r.Actor == nil || r.Actor.Generation != "2" || r.Actor.Key != first.Actor.Key {
		t.Fatal("safety-only prompt did not retain its reserved ActorRef", r)
	}
	if current := h.actor(first.Actor); current.Ref.Generation != "1" || current.Health != "stale" {
		t.Fatal("rejected prompt B replaced or healed the generation-1 actor head", current)
	}
	c, tx := interopOpen(t, h.f, false, sqliteio.Read)
	reserved, found, err := sqliteReadActorGeneration(tx, *r.Actor)
	if err != nil || !found || reserved != *r.Actor {
		t.Fatal("rejected prompt B did not retain its separate generation-2 row", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	h.clockErr = nil
	good := h.send(20, h.event("UserPromptSubmit", "C", ""))
	if good.Actor == nil || good.Actor.Generation != "3" || good.Actor.Key != r.Actor.Key {
		t.Fatal("later prompt reused rejected generation", good)
	}
	h.send(30, h.event("Stop", "B", ""))
	if h.actor(good.Actor).State != "working" {
		t.Fatal("late stop of rejected B closed current C")
	}
}

func TestSQLiteHostIngressH07PolicyLossPreservesCapturedContext(t *testing.T) {
	h := hiQANew(t, "codex", 2)
	h.start()
	root := h.send(0, h.event("UserPromptSubmit", "root", ""))
	h.revoke()
	e := h.event("Stop", "root", "")
	e.CWD = h.cwd[1]
	r := h.send(20, e)
	a := h.actor(root.Actor)
	if r.Disposition != "review_required" || a.Health != "stale" || a.Attribution.ProjectID != "3" || !reflect.DeepEqual(r.Actor, root.Actor) {
		t.Fatal("policy loss/CWD change silently reattributed live actor", r, a)
	}
	s := h.snapshot()
	if len(s.Rows["uncertainties"]) != 1 || len(s.Rows["intervals"]) != 0 || len(s.Rows["outbox"]) != 0 {
		t.Fatal("policy loss finalized an unproved working tail")
	}
}

func TestSQLiteHostIngressH08ClaudeQuestionOverlapExcludesWaiting(t *testing.T) {
	h := hiQANew(t, "claude", 1)
	h.start()
	root := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
	question := h.event("PreToolUse", "prompt", "")
	question.ToolID, question.ToolName = "question", "AskUserQuestion"
	h.send(10, question)
	if h.actor(root.Actor).State != "wait_user" {
		t.Fatal("question did not establish known wait")
	}
	ordinary := h.event("PreToolUse", "prompt", "")
	ordinary.ToolID, ordinary.ToolName = "ordinary", "Bash"
	h.send(20, ordinary)
	if h.actor(root.Actor).State != "working" {
		t.Fatal("ordinary overlapping tool did not resume work")
	}
	ordinary.Kind = "PostToolUse"
	h.send(25, ordinary)
	if h.actor(root.Actor).State != "wait_user" {
		t.Fatal("pending question was lost after ordinary completion")
	}
	question.Kind = "PostToolUse"
	h.send(30, question)
	if h.actor(root.Actor).State != "working" {
		t.Fatal("continuous matched question completion did not resume")
	}
	h.send(40, h.event("Stop", "prompt", ""))
	h.intervals("3", [2]int64{0, 10}, [2]int64{20, 25}, [2]int64{30, 40})
}

func TestSQLiteHostIngressH09MatchedQuestionDiscontinuityCannotResume(t *testing.T) {
	for _, mode := range []string{"epoch", "suspend"} {
		t.Run(mode, func(t *testing.T) {
			h := hiQANew(t, "claude", 1)
			h.start()
			root := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
			e := h.event("PreToolUse", "prompt", "")
			e.ToolID, e.ToolName = "question", "AskUserQuestion"
			h.send(10, e)
			h.at(30)
			if mode == "epoch" {
				epoch, n := "boot-2", "0"
				h.sample.Epoch, h.sample.ElapsedNS, h.sample.AwakeNS = &epoch, &n, &n
			} else {
				awake := strconv.FormatInt(10*int64(time.Second), 10)
				h.sample.AwakeNS = &awake
			}
			e.Kind = "PostToolUse"
			r, _ := h.s.ingestHostSQLite(context.Background(), e)
			a := h.actor(root.Actor)
			if r.Durability != "committed" || a.State == "working" || a.Health == "continuous" {
				t.Fatal("matched post erased PRE evidence and resumed across discontinuity", r, a)
			}
			h.intervals("3", [2]int64{0, 10})
			before := h.snapshot()
			if len(before.Rows["uncertainties"]) != 0 {
				t.Fatal("known idle question wait became working uncertainty")
			}
			h.reopen()
			_, _ = h.s.ingestHostSQLite(context.Background(), e)
			if h.actor(root.Actor).Health == "continuous" {
				t.Fatal("late matching replay healed lost question continuity")
			}
		})
	}
}

func TestSQLiteHostIngressH10ConflictReplaysOriginalTargetAndError(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	h.start()
	root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	e := h.event("PreToolUse", "turn", "")
	e.ToolID, e.ToolName = "tool", "Bash"
	h.send(10, e)
	e.ToolName = "wait_agent"
	h.at(20)
	r, err := h.s.ingestHostSQLite(context.Background(), e)
	qaCode(t, err, "event_conflict")
	if r.Durability != "committed" || !reflect.DeepEqual(r.Actor, root.Actor) || h.actor(root.Actor).Health != "stale" {
		t.Fatal("conflicting payload redirected original actor safety", r)
	}
	before := h.snapshot()
	h.reopen()
	h.clockErr = errors.New("conflict replay must precede current clock")
	got, err := h.s.ingestHostSQLite(context.Background(), e)
	qaCode(t, err, "event_conflict")
	if got.Disposition != "duplicate" || got.ID != r.ID || !reflect.DeepEqual(got.Actor, r.Actor) {
		t.Fatal("conflict exact error receipt was not retained")
	}
	hiQANonceOnly(t, before, h.snapshot())
}

func TestSQLiteHostIngressH11ResumeCapsOnlyRootUnknownTail(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	h.start()
	root := h.send(0, h.event("UserPromptSubmit", "root", ""))
	child := h.send(10, h.event("SubagentStart", "child", "child"))
	e := h.event("SessionStart", "", "")
	e.SessionSource = "resume"
	h.send(20, e)
	if h.actor(root.Actor).State != "interrupted" || h.actor(child.Actor).State != "working" || h.actor(child.Actor).Health != "stale" {
		t.Fatal("resume detached child or retained root as current work")
	}
	s := h.snapshot()
	if len(s.Rows["uncertainties"]) != 2 || len(s.Rows["intervals"]) != 0 {
		t.Fatal("resume lost conservative tails or billed them")
	}
	c, tx := interopOpen(t, h.f, false, sqliteio.Read)
	rows, _ := mqQAScan(t, tx, "SELECT actor_key,upper_bound_sec FROM uncertainties ORDER BY actor_key")
	for _, row := range rows {
		if row[0] == actorKey(root.Actor.Key) && row[1] == nil || row[0] == actorKey(child.Actor.Key) && row[1] != nil {
			t.Fatal("resume root/child cap boundary reversed")
		}
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteHostIngressH12BindingChangeDuringPreparedClockIsRechecked(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	h.start()
	before := h.snapshot()
	fired := false
	h.s.clock = ClockFunc(func() (ClockSample, error) {
		if !fired {
			fired = true
			// This injected callback runs outside SQL ownership. Native admission
			// and one genuine competing write prove that boundary and fact recheck.
			c, tx := interopOpen(t, h.f, false, sqliteio.Write)
			interopDone(t, tx, "UPDATE bindings SET task_id='5' WHERE binding_id=?", sqliteio.Text(spQAID(1)))
			interopCommit(t, tx)
			interopClose(t, c)
		}
		return h.sample, nil
	})
	r, err := h.s.ingestHostSQLite(context.Background(), h.event("UserPromptSubmit", "recheck", ""))
	qaCode(t, err, "state_busy")
	if !fired || r.Durability == "committed" {
		t.Fatal("stale binding decision committed")
	}
	after := h.snapshot()
	if len(after.Rows["actors"]) != 0 || len(after.Rows["event_receipts"]) != 0 || !reflect.DeepEqual(before.Rows["host_receipts"], after.Rows["host_receipts"]) {
		t.Fatal("stale prepared host decision left partial effects")
	}
}
