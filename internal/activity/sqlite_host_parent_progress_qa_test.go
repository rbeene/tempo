//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Parent work can commit after the first child-start host-fact read closes.
// The public resolver callback is an existing external seam at that point;
// it owns no SQL transaction. The child's later capture and clock reads see
// the newly committed parent. This exercises the real native writer recheck.
func TestSQLiteHostChildStartAfterConcurrentSameParentProgress(t *testing.T) {
	for _, change := range []string{"PostToolUse", "Stop"} {
		t.Run(change, func(t *testing.T) {
			h := hiQANew(t, "codex", 1)
			hxQAPublic(h)
			start := h.event("SessionStart", "", "")
			start.SessionSource = "startup"
			hxQASend(h, 0, start)
			root := hxQASend(h, 0, h.event("UserPromptSubmit", "root-turn", ""))
			if root.Actor == nil {
				t.Fatal("real root prompt did not establish its actor")
			}
			update := h.event(change, "root-turn", "")
			if change == "PostToolUse" {
				update.ToolID, update.ToolName = "spawn", "spawn_agent"
				pre := update
				pre.Kind = "PreToolUse"
				hxQASend(h, 5, pre)
			}
			before := h.actor(root.Actor)
			c, tx := interopOpen(t, h.f, false, sqliteio.Read)
			binding, found, readErr := sqliteReadBinding(tx, interopComputer, spQAID(1))
			interopRollback(t, tx)
			interopClose(t, c)
			if readErr != nil || !found || binding.Record == nil || binding.Record.Deleted {
				t.Fatal("real bound parent fixture missing", readErr)
			}
			peerClock := ClockFunc(func() (ClockSample, error) { return h.sample, nil })
			peer := NewSQLite(Options{Path: h.path, LockTimeout: sqliteFlowTestLockTimeout(), HookPolicies: h.policies, Clock: peerClock})
			fired := false
			var parentReceipt HostReceipt
			var parentErr error
			child := NewSQLite(Options{
				Path: h.path, LockTimeout: sqliteFlowTestLockTimeout(), HookPolicies: h.policies,
				Clock: ClockFunc(func() (ClockSample, error) { return h.sample, nil }),
				ResolveBinding: func(_ context.Context, e Event) (BindingSnapshot, bool, error) {
					if e.CWD != h.cwd[0] {
						return BindingSnapshot{}, false, errors.New("unexpected child binding lookup")
					}
					if !fired {
						fired = true
						h.at(10)
						parentReceipt, parentErr = peer.IngestHost(context.Background(), update)
					}
					return binding.Snapshot, true, parentErr
				},
			})
			event := h.event("SubagentStart", "child-turn", "child")
			childStarted := time.Now()
			got, err := child.IngestHost(context.Background(), event)
			childElapsed := time.Since(childStarted)
			childBudget := child.store.timeout
			if childBudget == 0 {
				childBudget = 250 * time.Millisecond
			}
			domain, bareDomain := err.(*Error)
			domainCode := ""
			if bareDomain {
				domainCode = domain.Code
			}
			_, bareNative := err.(*sqliteio.Error)
			var native *sqliteio.Error
			hasNative := errors.As(err, &native)
			t.Logf("child admission elapsed_us=%d budget_us=%d elapsed_at_least_budget=%t bare_domain=%t domain_code=%s bare_native=%t has_native=%t context_canceled=%t context_deadline=%t",
				childElapsed.Microseconds(), childBudget.Microseconds(), childElapsed >= childBudget, bareDomain, domainCode, bareNative, hasNative,
				errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded))
			if !fired || parentErr != nil || parentReceipt.Durability != "committed" || parentReceipt.Disposition != "applied" ||
				parentReceipt.Actor == nil || *parentReceipt.Actor != *root.Actor {
				t.Fatal("actual parent update did not commit during child preparation", fired, parentReceipt, parentErr)
			}
			current := h.actor(root.Actor)
			if current.Ref != before.Ref || current.Sequence == before.Sequence || current.BindingID != before.BindingID ||
				current.BindingRevision != before.BindingRevision || current.Attribution != before.Attribution {
				t.Fatal("fixture changed parent identity or did not advance its actor", before, current)
			}
			if change == "Stop" && current.State != "wait_user" || change == "PostToolUse" && current.State != "working" {
				t.Fatal("parent progress did not leave the expected nonterminal state", current.State)
			}
			if err != nil || got.Durability != "committed" || got.Disposition != "applied" || got.Actor == nil {
				t.Fatal("same-parent progress refused child start", got, err)
			}
			storedChild := h.actor(got.Actor)
			if storedChild.Parent == nil || *storedChild.Parent != *root.Actor || storedChild.Ref != *got.Actor ||
				storedChild.BindingID != current.BindingID || storedChild.BindingRevision != current.BindingRevision ||
				storedChild.Attribution != current.Attribution || storedChild.State != "working" {
				t.Fatal("child lost exact parent generation or inherited attribution", storedChild)
			}
			rows := h.snapshot().Rows
			wantReceipts := 4
			if change == "PostToolUse" {
				wantReceipts++
			}
			if len(rows["host_sessions"]) != 1 || len(rows["host_turns"]) != 2 || len(rows["host_receipts"]) != wantReceipts ||
				len(rows["actors"]) != 2 || len(rows["actor_generations"]) != 2 {
				t.Fatal("native host graph or receipt cardinality changed", rows)
			}
			beforeReplay := h.snapshot()
			replay, replayErr := child.IngestHost(context.Background(), event)
			if replayErr != nil || replay.Disposition != "duplicate" || replay.ID != got.ID || !reflect.DeepEqual(replay.Actor, got.Actor) {
				t.Fatal("exact child-start replay did not retain the committed identity", replay, replayErr)
			}
			hiQANonceOnly(t, beforeReplay, h.snapshot())
		})
	}
}
