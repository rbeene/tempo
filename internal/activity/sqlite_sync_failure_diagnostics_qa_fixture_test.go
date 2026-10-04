//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sfdQAPrefix = "tempo sync failure diagnostics v1: "
const sfdQAMax = int64(9007199254740991)

var sfdQAPhases = [12]string{"admission", "observe", "terminal_replay", "guard_acquire", "guard_verify_initial", "wal_maintenance", "reserve", "submit_roots", "before_complete_barrier", "guard_verify_final", "complete", "guard_cleanup"}

// Independent wire shape; no producer struct fields or validation helper are
// reused by this decoder or by the private test-only failure formatter.
type sfdQARecord struct {
	Version           int       `json:"schema_version"`
	Source            string    `json:"source"`
	Phase             [24]int64 `json:"phase_us"`
	Failed            [12]bool  `json:"phase_failed"`
	First             string    `json:"first_failed_phase"`
	CleanupFailed     bool      `json:"guard_cleanup_failed"`
	CloseCalls        int       `json:"guard_close_calls"`
	Initial           int64     `json:"initial_admission_deadline_us"`
	Completion        int64     `json:"completion_admission_deadline_us"`
	InitialExpired    *bool     `json:"initial_deadline_expired_at_return"`
	CompletionExpired *bool     `json:"completion_deadline_expired_at_return"`
	Caller            string    `json:"caller_at_return"`
	State             string    `json:"result_state"`
	Attempted         int64     `json:"attempted_count"`
	Remaining         int64     `json:"remaining_count"`
	Error             bool      `json:"final_error_present"`
	Dropped           bool      `json:"dropped"`
}

func sfdQADecode(b []byte) (r sfdQARecord, ok bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil || len(fields) != 17 {
		return r, false
	}
	for key, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) && key != "initial_deadline_expired_at_return" && key != "completion_deadline_expired_at_return" {
			return r, false
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&r) != nil {
		return r, false
	}
	var phases []int64
	var failed []bool
	if json.Unmarshal(fields["phase_us"], &phases) != nil || len(phases) != 24 || json.Unmarshal(fields["phase_failed"], &failed) != nil || len(failed) != 12 {
		return r, false
	}
	if r.Version != 1 || r.Source != "sync_now_private_phase" || r.Dropped || r.CloseCalls < 0 || r.CloseCalls > 2 || r.Attempted < -1 || r.Attempted > 100 || r.Remaining < -1 || r.Remaining > sfdQAMax {
		return r, false
	}
	if r.Initial < -sfdQAMax || r.Initial > sfdQAMax || r.Completion < -sfdQAMax || r.Completion > sfdQAMax || r.InitialExpired == nil && r.Initial != -1 || r.CompletionExpired == nil && r.Completion != -1 {
		return r, false
	}
	if r.Caller != "live" && r.Caller != "canceled" && r.Caller != "deadline" {
		return r, false
	}
	if r.State != "none" && r.State != "complete" && r.State != "interrupted" && r.State != "other" {
		return r, false
	}
	first := "none"
	for i, name := range sfdQAPhases {
		start, end := r.Phase[2*i], r.Phase[2*i+1]
		if start < -1 || start > sfdQAMax || end < -1 || end > sfdQAMax || (start == -1) != (end == -1) || end < start || start == -1 && r.Failed[i] {
			return r, false
		}
		if r.Failed[i] && first == "none" {
			first = name
		}
	}
	if r.First != first || r.CleanupFailed != r.Failed[11] {
		return r, false
	}
	return r, true
}

// Only the original wrong-project test calls this after SyncNow returns.
// The formatter accepts no error or payload and cannot replace its assertion.
func sqliteSyncFailureDiagnosticLog(d *sqliteSyncFailureDiagnostics, failed bool, postsUnchanged bool) string {
	if !failed {
		return ""
	}
	unavailable := sfdQAPrefix + "unavailable"
	b, err := json.Marshal(d.snapshot())
	if err != nil {
		return unavailable
	}
	r, valid := sfdQADecode(b)
	if !valid || !r.Error {
		return unavailable
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil {
		return unavailable
	}
	if postsUnchanged {
		fields["provider_posts_unchanged"] = json.RawMessage("true")
	} else {
		fields["provider_posts_unchanged"] = json.RawMessage("false")
	}
	b, err = json.Marshal(fields)
	if err != nil || len(sfdQAPrefix)+len(b) > 4096 {
		return unavailable
	}
	return sfdQAPrefix + string(b)
}

func sfdQAReadRecord(t *testing.T, d *sqliteSyncFailureDiagnostics) sfdQARecord {
	t.Helper()
	b, err := json.Marshal(d.snapshot())
	r, valid := sfdQADecode(b)
	if err != nil || !valid {
		t.Fatal("invalid fixed diagnostic snapshot")
	}
	for _, canary := range []string{"PRIVATE-PATH", "PRIVATE-REQUEST", "PRIVATE-ERROR", "PRIVATE-STATE", "SELECT PRIVATE"} {
		if strings.Contains(string(b), canary) {
			t.Fatal("private canary escaped into diagnostic")
		}
	}
	return r
}

// Only preexisting SQL/FS seams drive these independent controls. The new
// collector is never used to select a fault or decide which owner to release.
type sfdQAFaults struct {
	name                                                     string
	guardAcquired, guardReleased, rootAcquired, rootReleased atomic.Int64
	primary, cleanup, barrier, closeBefore, closeAfter       atomic.Int64
	completeArmed                                            atomic.Bool
	primaryError                                             *sqliteio.Error
	cleanupError                                             *sqliteio.Error
}

func sfdQAInstall(t *testing.T, s *Service, name string, cancel context.CancelFunc) *sfdQAFaults {
	t.Helper()
	p := &sfdQAFaults{name: name,
		primaryError: &sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.IO, Code: 10},
		cleanupError: &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Code: 10},
	}
	ncQASetFSHooks(ncQAFSHooks{Observe: func(e ncQAFSEvent) {
		if e.Op != "lease" {
			return
		}
		switch {
		case e.Role == "root" && e.Phase == "acquired":
			p.rootAcquired.Add(1)
		case e.Role == "root" && e.Phase == "released":
			p.rootReleased.Add(1)
		case e.Role == "sync-guard" && e.Phase == "acquired":
			p.guardAcquired.Add(1)
		case e.Role == "sync-guard" && e.Phase == "released":
			p.guardReleased.Add(1)
		}
	}})
	spQASetSQLHooks(spQASQLHooks{Fault: func(e spQASQLEvent) error {
		if e.Operation == "sync-guard" && e.Phase == "close-before" {
			p.closeBefore.Add(1)
		}
		if e.Operation == "sync-guard" && e.Phase == "close-after" {
			p.closeAfter.Add(1)
		}
		if ((name == "cleanup-before" || name == "primary-cleanup") && e.Operation == "sync-guard" && e.Phase == "close-before" || name == "cleanup-after" && e.Operation == "sync-guard" && e.Phase == "close-after") && p.cleanup.CompareAndSwap(0, 1) {
			return p.cleanupError
		}
		wanted := name == "observe" && e.Phase == "prepare-before" ||
			name == "maintenance" && p.guardAcquired.Load() == 1 && e.Phase == "prepare-before" ||
			name == "reserve" && e.Phase == "handoff-before-writer" ||
			name == "complete" && p.completeArmed.Load() && e.Phase == "handoff-before-writer"
		if wanted && p.primary.CompareAndSwap(0, 1) {
			return p.primaryError
		}
		return nil
	}})
	s.store.fail = func(stage string) error {
		if stage != "sync_before_complete" {
			return nil
		}
		p.barrier.Add(1)
		if name == "complete" {
			p.completeArmed.Store(true)
		}
		if name == "cancel-at-barrier" {
			cancel()
		}
		if name == "barrier" || name == "primary-cleanup" {
			return p.primaryError
		}
		return nil
	}
	t.Cleanup(func() { sfdQAClear(s) })
	return p
}

func sfdQAClear(s *Service) {
	spQASetSQLHooks(spQASQLHooks{})
	ncQASetFSHooks(ncQAFSHooks{})
	s.store.fail = nil
}

func (p *sfdQAFaults) check(t *testing.T) {
	t.Helper()
	if p.rootAcquired.Load() != p.rootReleased.Load() {
		t.Fatal("diagnostic control retained a native root lease")
	}
	if p.name == "guard-acquire" {
		// Failed native acquisition closes a cleanup-only owner and emits its
		// release even though no successful lease-acquired event was emitted.
		if p.guardAcquired.Load() != 0 || p.guardReleased.Load() != 1 {
			t.Fatal("actual held-guard refusal/cleanup not reached")
		}
	} else if p.guardAcquired.Load() != p.guardReleased.Load() {
		t.Fatal("diagnostic control retained a sync guard lease")
	}
	switch p.name {
	case "observe", "maintenance", "reserve", "complete":
		if p.primary.Load() != 1 {
			t.Fatal("required actual native fault boundary was not reached")
		}
	}
	if p.name == "cleanup-before" || p.name == "primary-cleanup" {
		if p.cleanup.Load() != 1 || p.closeBefore.Load() != 2 || p.closeAfter.Load() != 1 {
			t.Fatal("one-shot preclose failure did not finish same native owner on existing second call")
		}
	}
	if p.name == "cleanup-after" {
		if p.cleanup.Load() != 1 || p.closeBefore.Load() != 1 || p.closeAfter.Load() != 1 {
			t.Fatal("terminal close failure repeated native cleanup")
		}
	}
	if p.name == "barrier" || p.name == "primary-cleanup" || p.name == "complete" || p.name == "cancel-at-barrier" {
		if p.barrier.Load() != 1 {
			t.Fatal("actual before-complete boundary not reached exactly once")
		}
	}
}

// Each run is checked against its own complete cold graph. Random binding and
// computer IDs from separate public fixtures are never normalized away from
// these authoritative checks or compared as though they were the same state.
func sfdQADelta(t *testing.T, before, after scvQASnapshot, in SyncRunInput, kind string, result SyncRun) {
	t.Helper()
	if kind == "unchanged" {
		scvQAUnchanged(t, before, after)
		return
	}
	if kind == "replay" {
		scvQANonceOnly(t, before, after)
		return
	}
	commits := 1
	if kind == "complete" {
		commits = 2
	}
	want := before.meta
	for i := 0; i < commits; i++ {
		want.Revision = bump(want.Revision)
		var err error
		want.DurabilityNonce, err = sqliteNextNonce(want.DurabilityNonce[:])
		if err != nil {
			t.Fatal("cold expected nonce increment", err)
		}
	}
	want.LogicalBytes = after.meta.LogicalBytes
	if !reflect.DeepEqual(want, after.meta) || after.meta.LogicalBytes <= before.meta.LogicalBytes || !reflect.DeepEqual(before.configs, after.configs) {
		t.Fatal("observation changed metadata/configuration or missed exact durable reservation/completion count")
	}
	if kind == "pending" {
		// This public zero-root fixture has no prior pending reservation. Pin
		// every normalized column independently of the typed receipt below.
		revision, err := strconv.ParseUint(bump(before.meta.Revision), 10, 64)
		if err != nil {
			t.Fatal("expected pending snapshot revision", err)
		}
		wantPending := [][]string{{
			fmt.Sprintf("%v:%s", sqliteio.TextKind, in.RequestID),
			fmt.Sprintf("%v:1", sqliteio.IntegerKind),
			fmt.Sprintf("%v:run", sqliteio.TextKind),
			fmt.Sprintf("%v:0", sqliteio.IntegerKind),
			fmt.Sprintf("%v:%016x", sqliteio.BlobKind, revision),
			fmt.Sprintf("%v:20", sqliteio.IntegerKind),
			fmt.Sprintf("%v:NULL", sqliteio.NullKind),
			fmt.Sprintf("%v:NULL", sqliteio.NullKind),
			fmt.Sprintf("%v:NULL", sqliteio.NullKind),
			fmt.Sprintf("%v:NULL", sqliteio.NullKind),
			fmt.Sprintf("%v:NULL", sqliteio.NullKind),
		}}
		if len(before.rows["pending_sync"]) != 0 || !reflect.DeepEqual(after.rows["pending_sync"], wantPending) || len(before.rows["pending_sync_roots"]) != 0 || len(after.rows["pending_sync_roots"]) != 0 {
			t.Fatal("failure changed the exact normalized zero-root reservation")
		}
	}
	for table, rows := range before.rows {
		if table == "pending_sync" && kind == "pending" {
			continue // The complete replacement image was checked above.
		}
		if table != "store_meta" && table != "requests" && !reflect.DeepEqual(rows, after.rows[table]) {
			t.Fatal("observation changed an unrelated retained table", table)
		}
	}
	oldRows := [][]string{}
	for _, row := range after.rows["requests"] {
		if row[0] != fmt.Sprintf("%v:%s", sqliteio.TextKind, in.RequestID) {
			oldRows = append(oldRows, row)
		}
	}
	if len(after.rows["requests"]) != len(before.rows["requests"])+1 || !reflect.DeepEqual(before.rows["requests"], oldRows) {
		t.Fatal("observation rewrote old receipts or created extra requests")
	}
	row, found := after.requests[in.RequestID]
	in.Limit = 20
	if !found || row.ID != in.RequestID || row.Value.Operation != "sync.now" || row.Value.Fingerprint != mutationFingerprint("sync.now", in) || row.Value.Error != nil {
		t.Fatal("actual new sync receipt identity changed")
	}
	if kind == "pending" {
		v := row.Value.PendingSync
		if v == nil || v.Run == nil || !reflect.DeepEqual(*v.Run, in) || v.RootIDs == nil || len(v.RootIDs) != 0 || v.SnapshotRevision != after.meta.Revision || v.EffectCommitted || row.Value.SyncRun != nil {
			t.Fatal("failure lost honest zero-root reservation")
		}
	} else {
		if row.Value.PendingSync != nil || row.Value.SyncRun == nil {
			t.Fatal("completed durable zero-root run lost its terminal receipt")
		}
		stored := *row.Value.SyncRun
		if stored.ContractVersion != 1 || stored.RequestID != in.RequestID || stored.SnapshotRevision != after.meta.Revision || stored.State != "complete" || stored.AttemptedIDs == nil || len(stored.AttemptedIDs) != 0 || stored.ResolvedIDs == nil || len(stored.ResolvedIDs) != 0 || stored.BlockedIDs == nil || len(stored.BlockedIDs) != 0 || stored.RemainingCount != 0 {
			t.Fatal("zero-root completion fabricated work")
		}
		if result.State != "" && !reflect.DeepEqual(result, stored) {
			t.Fatal("public successful result differs from its durable receipt")
		}
	}
}

func sfdQACleanupEvidence(t *testing.T, err error, primary bool) {
	t.Helper()
	var guarded *sqliteSyncNowGuardError
	if !errors.As(err, &guarded) || guarded.owner == nil || !guarded.owner.closed || guarded.owner.native != nil || guarded.owner.closeErr == nil {
		t.Fatal("original terminal guard-error owner/evidence was discarded")
	}
	var native *sqliteio.Error
	if !errors.As(guarded.evidence, &native) || native.Phase != sqliteio.ClosePhase || native.Category != sqliteio.IO || native.Code != 10 {
		t.Fatal("first cleanup failure evidence was erased")
	}
	if primary {
		var domain *Error
		if !errors.As(guarded.evidence, &domain) || domain.Code != "local_write_unknown" {
			t.Fatal("cleanup erased the original barrier failure")
		}
	}
}

func sfdQAHeldGuard(t *testing.T, s *Service) *sqliteSyncRunGuard {
	t.Helper()
	g, err := s.acquireSQLiteSyncLock(context.Background(), time.Now().Add(250*time.Millisecond))
	if err != nil || g == nil {
		t.Fatal("SETUP actual held sync guard", err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error("held sync guard cleanup", err)
		}
	})
	return g
}
