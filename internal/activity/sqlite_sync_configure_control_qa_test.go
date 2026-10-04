//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteSyncConfigurePublicAtomicRowsAndHistoricalReplay(t *testing.T) {
	s, f := scvQABootstrap(t)
	in := scvQAInput(1)
	p := qaNewSyncProvider(t)
	before := scvQAAudit(t, f, in.RequestID)
	started := time.Now().UTC()
	first, err := s.SyncConfigure(context.Background(), in, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal("public configure", err)
	}
	if first.ContractVersion != 1 || first.RequestID != in.RequestID || first.SnapshotRevision != "2" || !first.Changed || first.Configuration.AccountID != "1" || first.Configuration.UserID != "2" || first.Configuration.Revision != "1" || first.Configuration.Mode != "duration" || first.Configuration.Clock != nil || first.Configuration.DurationPolicy != "exact" || first.Configuration.PolicyVersion != "exact-v1" || !first.Configuration.Declared || first.Configuration.Source != "user_declared" || first.Configuration.DeclaredAt.Before(started) || first.Configuration.DeclaredAt.After(time.Now().UTC()) {
		t.Fatal("configuration result contract", first)
	}
	after := scvQAAudit(t, f, in.RequestID)
	if after.meta.Revision != "2" || after.meta.SyncEnabled || len(after.configs) != 1 || !reflect.DeepEqual(after.configs[0], first.Configuration) || !reflect.DeepEqual(after.requests[in.RequestID].Value.SyncConfigurationResult, &first) || after.requests[in.RequestID].Value.Fingerprint != mutationFingerprint("sync.configure", in) || after.meta.LogicalBytes <= before.meta.LogicalBytes {
		t.Fatal("atomic metadata/configuration/typed receipt", after.meta)
	}
	if !reflect.DeepEqual(p.calls, []string{"accounts", "/users/me"}) || len(p.posts) != 0 {
		t.Fatal("configure did more than current-user discovery", p.calls)
	}
	// A later, distinct configuration must not be resurrected by old replay.
	secondInput := scvQAInput(2)
	secondInput.IfRevision, secondInput.Mode, secondInput.Clock = "1", "timestamp", "24h"
	second, err := scvQAService(t, f).SyncConfigure(context.Background(), secondInput, qaSyncDeps(t, p))
	if err != nil || second.Configuration.Revision != "2" || second.SnapshotRevision != "3" || second.Configuration.Clock == nil {
		t.Fatal("second public configuration", second, err)
	}
	*second.Configuration.Clock = "12h" // returned pointer must be owned
	beforeReplay := scvQAAudit(t, f, in.RequestID, secondInput.RequestID)
	if beforeReplay.configs[0].Clock == nil || *beforeReplay.configs[0].Clock != "24h" {
		t.Fatal("caller changed stored configuration")
	}
	replay, err := scvQAService(t, f).SyncConfigure(context.Background(), in, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(replay, first) {
		t.Fatal("historical offline replay", replay, err)
	}
	scvQANonceOnly(t, beforeReplay, scvQAAudit(t, f, in.RequestID, secondInput.RequestID))
	mismatch := in
	mismatch.UserID = "" // distinct fingerprint even though discovered user is 2
	r, err := s.SyncConfigure(context.Background(), mismatch, qaSyncNoProvider(t))
	scvQAError(t, r, SyncConfigurationResult{}, err, "request_conflict", in.RequestID)
}

func TestSQLiteSyncControlPublicNoopReceiptsAndHistoricalReplay(t *testing.T) {
	s, f := scvQABootstrap(t)
	ids := []string{scvQAID(10), scvQAID(11), scvQAID(12), scvQAID(13)}
	var first MutationResult
	for i, enabled := range []bool{true, true, false, false} {
		var r MutationResult
		var err error
		if enabled {
			r, err = s.SyncResume(context.Background(), ids[i])
		} else {
			r, err = s.SyncPause(context.Background(), ids[i])
		}
		wantRevision := []string{"2", "3", "4", "5"}[i]
		if err != nil || r.ContractVersion != 1 || r.RequestID != ids[i] || r.SnapshotRevision != wantRevision || r.Changed != (i%2 == 0) || r.AffectedIDs == nil || len(r.AffectedIDs) != 0 || r.EntityRevision != nil {
			t.Fatal("control exact new/noop result", i, r, err)
		}
		if i == 0 {
			first = r
		}
		cold := scvQAAudit(t, f, ids[:i+1]...)
		if cold.meta.Revision != wantRevision || cold.meta.SyncEnabled != enabled || len(cold.configs) != 0 || !reflect.DeepEqual(cold.requests[ids[i]].Value.MutationResult, &r) {
			t.Fatal("control rows/receipt/metadata not atomic", i)
		}
	}
	before := scvQAAudit(t, f, ids...)
	replay, err := scvQAService(t, f).SyncResume(context.Background(), ids[0])
	if err != nil || !reflect.DeepEqual(replay, first) {
		t.Fatal("old Resume receipt changed", replay, err)
	}
	after := scvQAAudit(t, f, ids...)
	if after.meta.SyncEnabled {
		t.Fatal("historical Resume replay re-enabled current paused store")
	}
	scvQANonceOnly(t, before, after)
	conflict, err := s.SyncPause(context.Background(), ids[0])
	scvQAError(t, conflict, MutationResult{}, err, "request_conflict", ids[0])
	scvQAUnchanged(t, after, scvQAAudit(t, f, ids...))
}

func TestSQLiteSyncConfigureControlValidateAndRequireExistingFreshAuthority(t *testing.T) {
	for _, operation := range []string{"configure", "resume", "pause"} {
		t.Run(operation, func(t *testing.T) {
			f := interopLocation(t)
			f.directory = filepath.Join(f.directory, "absent")
			s := scvQAService(t, f)
			if operation == "configure" {
				bad := scvQAInput(20)
				bad.Confirmed = false
				r, err := s.SyncConfigure(context.Background(), bad, qaSyncNoProvider(t))
				scvQAError(t, r, SyncConfigurationResult{}, err, "confirmation_required", bad.RequestID)
				r, err = s.SyncConfigure(context.Background(), scvQAInput(20), qaSyncNoProvider(t))
				scvQAError(t, r, SyncConfigurationResult{}, err, "input_required", scvQAID(20))
			} else {
				call := s.SyncResume
				if operation == "pause" {
					call = s.SyncPause
				}
				r, err := call(context.Background(), "not-a-uuid")
				scvQAError(t, r, MutationResult{}, err, "validation", "")
				r, err = call(context.Background(), scvQAID(20))
				scvQAError(t, r, MutationResult{}, err, "input_required", scvQAID(20))
			}
			if _, err := os.Lstat(f.directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("noncreating refusal touched directory", err)
			}
		})
	}
	f := interopLocation(t)
	path := filepath.Join(f.directory, f.authority)
	original := []byte("occupied user authority must remain opaque\n")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	s := scvQAService(t, f)
	r, err := s.SyncConfigure(context.Background(), scvQAInput(21), qaSyncNoProvider(t))
	scvQAError(t, r, SyncConfigurationResult{}, err, "state_path_in_use", scvQAID(21))
	control, err := s.SyncResume(context.Background(), scvQAID(22))
	scvQAError(t, control, MutationResult{}, err, "state_path_in_use", scvQAID(22))
	actual, err := os.ReadFile(path)
	entries, dirErr := os.ReadDir(f.directory)
	if err != nil || dirErr != nil || string(actual) != string(original) || len(entries) != 1 {
		t.Fatal("occupied P parsed/replaced or created SQLite/guard", err, dirErr)
	}
}

func TestSQLiteSyncConfigureProviderOutsideOwnershipAndCASRecheck(t *testing.T) {
	s, f := scvQABootstrap(t)
	outer, inner := scvQAInput(30), scvQAInput(31)
	p := qaNewSyncProvider(t)
	var winner SyncConfigurationResult
	called := false
	p.beforeUser = func() {
		called = true
		// A real public writer must complete while the outer provider callback
		// runs. This detects retained SQL ownership, not merely a mock flag.
		var err error
		winner, err = scvQAService(t, f).SyncConfigure(context.Background(), inner, qaSyncDeps(t, qaNewSyncProvider(t)))
		if err != nil {
			t.Fatal("provider callback could not complete independent writer", err)
		}
	}
	r, err := s.SyncConfigure(context.Background(), outer, qaSyncDeps(t, p))
	scvQAError(t, r, SyncConfigurationResult{}, err, "revision_conflict", outer.RequestID)
	cold := scvQAAudit(t, f, outer.RequestID, inner.RequestID)
	if !called || cold.meta.Revision != "2" || len(cold.requests) != 1 || !reflect.DeepEqual(cold.requests[inner.RequestID].Value.SyncConfigurationResult, &winner) || len(p.posts) != 0 {
		t.Fatal("outer overwrote current config or saved a failed receipt")
	}
}

func TestSQLiteSyncConfigureIdentityAndProviderFailureLeaveNoReceipt(t *testing.T) {
	s, f := scvQABootstrap(t)
	for i, code := range []string{"identity_conflict", "network"} {
		in := scvQAInput(40 + i)
		p := qaNewSyncProvider(t)
		if i == 0 {
			in.UserID = "3"
		} else {
			p.userErr = errors.New("synthetic private transport detail")
		}
		before := scvQAAudit(t, f, in.RequestID)
		r, err := s.SyncConfigure(context.Background(), in, qaSyncDeps(t, p))
		scvQAError(t, r, SyncConfigurationResult{}, err, code, in.RequestID)
		scvQAUnchanged(t, before, scvQAAudit(t, f, in.RequestID))
		if len(p.posts) != 0 {
			t.Fatal("configure wrote to provider")
		}
	}
}

func TestSQLiteSyncConfigureControlDurableFailureRequiresExactReplayFence(t *testing.T) {
	for _, configure := range []bool{true, false} {
		t.Run(map[bool]string{true: "configure", false: "resume"}[configure], func(t *testing.T) {
			s, f := scvQABootstrap(t)
			in := scvQAInput(50)
			committed, nativeClosed, faulted := false, false, false
			spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
				if e.Phase == "commit-after-engine" && e.Code == 101 {
					committed = true
				}
				if e.Phase == "durable-native-closed" && e.Code == 0 {
					nativeClosed = true
				}
			}, Fault: func(e spQASQLEvent) error {
				if e.Phase == "fsync-before" && e.Operation == "durable-main" && !faulted {
					faulted = true
					return &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Code: 10}
				}
				return nil
			}})
			if configure {
				r, err := s.SyncConfigure(context.Background(), in, qaSyncDeps(t, qaNewSyncProvider(t)))
				scvQAError(t, r, SyncConfigurationResult{}, err, "local_write_unknown", in.RequestID)
			} else {
				r, err := s.SyncResume(context.Background(), in.RequestID)
				scvQAError(t, r, MutationResult{}, err, "local_write_unknown", in.RequestID)
			}
			spQASetSQLHooks(spQASQLHooks{})
			if !committed || !nativeClosed || !faulted {
				t.Fatal("fault did not follow actual COMMIT and native close", committed, nativeClosed, faulted)
			}
			before := scvQAAudit(t, f, in.RequestID)
			if before.meta.Revision != "2" || len(before.requests) != 1 || before.meta.SyncEnabled == configure {
				t.Fatal("committed fact vanished after barrier uncertainty")
			}
			if configure {
				r, err := scvQAService(t, f).SyncConfigure(context.Background(), in, qaSyncNoProvider(t))
				if err != nil || !reflect.DeepEqual(before.requests[in.RequestID].Value.SyncConfigurationResult, &r) {
					t.Fatal("exact unknown configure replay", r, err)
				}
			} else {
				r, err := scvQAService(t, f).SyncResume(context.Background(), in.RequestID)
				if err != nil || !reflect.DeepEqual(before.requests[in.RequestID].Value.MutationResult, &r) {
					t.Fatal("exact unknown resume replay", r, err)
				}
			}
			scvQANonceOnly(t, before, scvQAAudit(t, f, in.RequestID))
		})
	}
}

func TestSQLiteSyncConfigureControlPredispatchFaultRollsBackWholeUnit(t *testing.T) {
	for _, configure := range []bool{true, false} {
		t.Run(map[bool]string{true: "configure", false: "resume"}[configure], func(t *testing.T) {
			s, f := scvQABootstrap(t)
			in := scvQAInput(60)
			before := scvQAAudit(t, f, in.RequestID)
			faulted, rolledBack := false, false
			spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
				if faulted && e.Phase == "rollback-after" && e.Code == 0 {
					rolledBack = true
				}
			}, Fault: func(e spQASQLEvent) error {
				if e.Phase == "commit-before-dispatch" && !faulted {
					faulted = true
					return &sqliteio.Error{Phase: sqliteio.CommitPhase, Category: sqliteio.IO, Code: 10}
				}
				return nil
			}})
			if configure {
				r, err := s.SyncConfigure(context.Background(), in, qaSyncDeps(t, qaNewSyncProvider(t)))
				scvQAError(t, r, SyncConfigurationResult{}, err, "state_corrupt", in.RequestID)
			} else {
				r, err := s.SyncResume(context.Background(), in.RequestID)
				scvQAError(t, r, MutationResult{}, err, "state_corrupt", in.RequestID)
			}
			spQASetSQLHooks(spQASQLHooks{})
			if !faulted || !rolledBack {
				t.Fatal("no actual predispatch fault/rollback witness", faulted, rolledBack)
			}
			scvQAUnchanged(t, before, scvQAAudit(t, f, in.RequestID))
		})
	}
}

func TestSQLiteSyncConfigureControlUsesDefaultAdmissionAndCancellation(t *testing.T) {
	s, f := scvQABootstrap(t)
	in := scvQAInput(70)
	before := scvQAAudit(t, f, in.RequestID)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := s.SyncConfigure(ctx, in, qaSyncNoProvider(t))
	scvQAError(t, r, SyncConfigurationResult{}, err, "state_busy", in.RequestID)
	control, err := s.SyncResume(ctx, in.RequestID)
	scvQAError(t, control, MutationResult{}, err, "state_busy", in.RequestID)
	c, tx := scvQAReader(t, f) // actual same-store owner; no fake busy hook
	bounded, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()
	start := time.Now()
	r, err = s.SyncConfigure(bounded, in, qaSyncNoProvider(t))
	elapsed := time.Since(start)
	scvQAError(t, r, SyncConfigurationResult{}, err, "state_busy", in.RequestID)
	if elapsed < 180*time.Millisecond || elapsed > 750*time.Millisecond {
		t.Fatal("default 250 ms admission replaced by action budget", elapsed)
	}
	scvQAEnd(t, c, tx)
	scvQAUnchanged(t, before, scvQAAudit(t, f, in.RequestID))
}
