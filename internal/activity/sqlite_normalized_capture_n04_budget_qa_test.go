//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// The N04 semantic fixture may select a supported admission budget, but it
// cannot replace the caller's earlier deadline or ignore caller cancellation.
func TestSQLiteNormalizedCaptureN04SemanticBudgetPreservesCaller(t *testing.T) {
	for _, tc := range []struct {
		name        string
		wantContext error
	}{
		{"live_shorter_caller", nil},
		{"cancel_during_resolver", context.Canceled},
		{"deadline_during_resolver", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := ncQANew(t, nil)
			q.service.store.timeout = time.Second
			q.legacy.service.store.timeout = time.Second
			q.sample = ncQASample(10)
			q.legacy.sample = q.sample
			e := qaEvent("A", "1", "1", "work", qaBindingA)
			before, rows := ncQAAudit(t, q.f)
			var want EventResult
			if tc.wantContext == nil {
				var err error
				want, err = q.legacy.service.Ingest(context.Background(), e)
				if err != nil {
					t.Fatal("legacy semantic positive control", err)
				}
			}

			callerDeadline := time.Now().Add(900 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
			defer cancel()
			entered, afterResolver := false, false
			leaseAfter, beginAfter, commitAfter := false, false, false
			ncQASetFSHooks(ncQAFSHooks{Observe: func(ev ncQAFSEvent) {
				if afterResolver && ev.Role == "root" && ev.Op == "lease" && ev.Phase == "acquired" {
					leaseAfter = true
				}
			}})
			ncQAHooks(t, ncQASQLHooks{Observe: func(ev ncQASQLEvent) {
				if afterResolver && ev.Phase == "control-before-native" {
					if ev.Operation == "begin" {
						beginAfter = true
					}
					if ev.Operation == "commit" {
						commitAfter = true
					}
				}
			}})
			t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}) })
			resolve := q.service.resolve
			q.service.resolve = func(resolveCtx context.Context, event Event) (BindingSnapshot, bool, error) {
				entered = true
				deadline, ok := resolveCtx.Deadline()
				if !ok || !deadline.Equal(callerDeadline) || resolveCtx.Err() != nil {
					t.Error("fixture budget replaced the live shorter caller context")
				}
				binding, found, err := resolve(resolveCtx, event)
				switch tc.wantContext {
				case context.Canceled:
					cancel()
				case context.DeadlineExceeded:
					// Wait for the actual earlier caller deadline, not a sleep
					// or a synthetic replacement for the native capture result.
					<-resolveCtx.Done()
				}
				afterResolver = true
				return binding, found, err
			}
			got, err := q.service.ingestSQLite(ctx, e)
			ncQASetSQLHooks(ncQASQLHooks{})
			ncQASetFSHooks(ncQAFSHooks{})
			if !entered || q.resolverCalls != 1 {
				t.Fatal("actual resolver control was not reached exactly once")
			}
			if tc.wantContext != nil {
				if ctx.Err() != tc.wantContext || context.Cause(ctx) != tc.wantContext || ncQAErrorCode(err) != "state_busy" ||
					!reflect.DeepEqual(got, EventResult{}) || q.clockCalls != 0 || leaseAfter || beginAfter || commitAfter {
					t.Fatalf("caller termination lost: context=%v cause=%v want=%v err=%v samples=%d lease=%t begin=%t commit=%t", ctx.Err(), context.Cause(ctx), tc.wantContext, err, q.clockCalls, leaseAfter, beginAfter, commitAfter)
				}
				ncQAUnchanged(t, before, rows, q.f)
				return
			}
			if err != nil || q.clockCalls != 1 || !leaseAfter || !beginAfter || !commitAfter {
				t.Fatalf("live caller did not complete actual capture: err=%v samples=%d lease=%t begin=%t commit=%t", err, q.clockCalls, leaseAfter, beginAfter, commitAfter)
			}
			wantState := bgQAReadLegacy(t, q.legacy.service)
			gotState := ncQAReadState(t, q)
			if !reflect.DeepEqual(ncQACanonical(t, wantState, wantState), ncQACanonical(t, gotState, gotState)) {
				t.Fatal("live caller complete cold state differs from legacy oracle")
			}
			if !reflect.DeepEqual(ncQACanonical(t, wantState, want), ncQACanonical(t, gotState, got)) {
				t.Fatal("live caller public outcome differs from legacy oracle")
			}
			ncQAAudit(t, q.f)
		})
	}
}
