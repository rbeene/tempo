//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/harvest"
)

func (s *Service) syncNowSQLite(ctx context.Context, in SyncRunInput, d SyncDependencies) (result SyncRun, err error) {
	if ctx == nil || !validUUID(in.RequestID) || in.Limit < 0 || in.Limit > 100 {
		return result, failure("validation")
	}
	if in.Limit == 0 {
		in.Limit = 20
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	diagnostic := sqliteSyncFailureDiagnosticsFromContext(ctx)
	defer func() { diagnostic.finish(ctx, result, err != nil) }()
	diagnostic.begin(0)
	a, budget, err := s.sqliteSyncConfigAdmission(ctx)
	diagnostic.end(0, err != nil)
	if err != nil {
		return result, err
	}
	diagnostic.initialDeadline(a.AcquireDeadline)
	fp := mutationFingerprint("sync.now", in)
	diagnostic.begin(1)
	terminal, err := sqliteSyncNowObserve(ctx, a, in.RequestID, fp)
	diagnostic.end(1, err != nil)
	if err != nil {
		return result, err
	}
	if terminal {
		diagnostic.begin(2)
		result, err = sqliteSyncNowReplay(ctx, a, in.RequestID, fp)
		diagnostic.end(2, err != nil)
		return result, err
	}
	var guard *sqliteSyncRunGuard
	effect := false
	defer func() {
		diagnostic.begin(11)
		diagnostic.guardClose()
		closeErr := guard.Close()
		if closeErr != nil {
			// A cached terminal error is harmless to retry; a nonterminal close
			// retains and retries this exact owner once. Neither erases evidence.
			diagnostic.guardClose()
			retryErr := guard.Close()
			err = &sqliteSyncNowGuardError{public: requestError(failure("local_write_unknown"), in.RequestID), evidence: errors.Join(err, closeErr, retryErr), owner: guard}
		} else if err != nil && effect {
			err = sqliteLinkFailure(err, nil, nil, false, true, in.RequestID, nil)
		}
		if err != nil {
			result = SyncRun{}
		}
		diagnostic.end(11, closeErr != nil)
	}()
	diagnostic.begin(3)
	guard, err = s.acquireSQLiteSyncLock(ctx, a.AcquireDeadline)
	diagnostic.end(3, err != nil)
	if err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, false, in.RequestID, nil)
	}
	diagnostic.begin(4)
	err = guard.Verify()
	diagnostic.end(4, err != nil)
	if err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, false, in.RequestID, nil)
	}
	diagnostic.begin(5)
	err = sqliteSyncNowMaintainWAL(ctx, a, guard, in.RequestID)
	diagnostic.end(5, err != nil)
	if err != nil {
		return result, err
	}
	diagnostic.begin(6)
	roots, replay, err := sqliteSyncNowReserve(ctx, a, in, fp)
	diagnostic.end(6, err != nil)
	if err != nil {
		return result, err
	}
	if replay != nil {
		return *replay, nil
	}
	diagnostic.begin(7)
	for _, root := range roots {
		posted, submitErr := s.sqliteSyncNowSubmit(ctx, a, budget, guard, in.RequestID, root, d)
		effect = effect || posted
		if submitErr != nil {
			diagnostic.end(7, true)
			return result, submitErr
		}
	}
	diagnostic.end(7, false)
	diagnostic.begin(8)
	err = s.syncBarrier("sync_before_complete", in.RequestID)
	diagnostic.end(8, err != nil)
	if err != nil {
		return result, err
	}
	diagnostic.begin(9)
	err = guard.Verify()
	diagnostic.end(9, err != nil)
	if err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, effect, in.RequestID, nil)
	}
	a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
	diagnostic.completionDeadline(a.AcquireDeadline)
	diagnostic.begin(10)
	result, err = sqliteSyncNowComplete(ctx, a, in.RequestID, fp, roots)
	diagnostic.end(10, err != nil)
	return result, err
}

// Exceptional guard ownership remains reachable from the safe returned error;
// no global registry, detached cleanup or raw native diagnostics are exposed.
type sqliteSyncNowGuardError struct {
	public, evidence error
	owner            *sqliteSyncRunGuard
}

func (e *sqliteSyncNowGuardError) Error() string { return e.public.Error() }
func (e *sqliteSyncNowGuardError) Unwrap() error { return e.public }

func sqliteSyncNowReceipt(tx *sqliteio.Tx, meta sqliteStoreMeta, id, fp string) (sqliteMutationRequestRow, bool, error) {
	row, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, id, meta.Revision)
	if err != nil || !found {
		return sqliteMutationRequestRow{}, false, err
	}
	if row.Value.Operation != "sync.now" || row.Value.Fingerprint != fp {
		return sqliteMutationRequestRow{}, false, failure("request_conflict")
	}
	if row.Value.SyncRun == nil && (row.Value.PendingSync == nil || row.Value.PendingSync.Run == nil) {
		return sqliteMutationRequestRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteSyncNowObserve(ctx context.Context, a sqliteCaptureAdmission, id, fp string) (terminal bool, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	known := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, false, id, nil)
			terminal = false
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Read)
	if err != nil {
		return false, err
	}
	if !found {
		return false, syncRequired("binding")
	}
	row, found, err := sqliteSyncNowReceipt(tx, meta, id, fp)
	if err != nil || !found {
		return false, err
	}
	known = true
	return row.Value.SyncRun != nil, nil
}

func sqliteSyncNowReplay(ctx context.Context, a sqliteCaptureAdmission, id, fp string) (result SyncRun, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, true, uncertain, id, nil)
			result = SyncRun{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	row, found, err := sqliteSyncNowReceipt(tx, meta, id, fp)
	if err != nil {
		return result, err
	}
	if !found || row.Value.SyncRun == nil {
		return result, failure("state_corrupt")
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return result, err
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, meta, nil, []string{id})
	if err != nil {
		return result, err
	}
	return *row.Value.SyncRun, nil
}

func sqliteSyncNowReserve(ctx context.Context, a sqliteCaptureAdmission, in SyncRunInput, fp string) (roots []string, replay *SyncRun, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	known, uncertain := false, false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, uncertain, in.RequestID, nil)
			roots, replay = nil, nil
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		return nil, nil, syncRequired("binding")
	}
	prior, found, err := sqliteSyncNowReceipt(tx, meta, in.RequestID, fp)
	if err != nil {
		return nil, nil, err
	}
	known = found
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return nil, nil, err
	}
	if found && prior.Value.SyncRun != nil {
		uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, meta, nil, []string{in.RequestID})
		if err != nil {
			return nil, nil, err
		}
		return []string{}, prior.Value.SyncRun, nil
	}
	if meta.Revision == "18446744073709551615" {
		return nil, nil, failure("validation")
	}
	after := meta
	after.Revision = bump(meta.Revision)
	changedRoots, requests := []string{}, []string{in.RequestID}
	if err = sqliteSyncRecoverPending(tx, meta, &after, &changedRoots, &requests); err != nil {
		return nil, nil, err
	}
	prior, found, err = sqliteSyncNowReceipt(tx, after, in.RequestID, fp)
	if err != nil {
		return nil, nil, err
	}
	if found {
		known = true
		if prior.Value.SyncRun == nil {
			return nil, nil, failure("state_corrupt")
		}
		replay = prior.Value.SyncRun
	} else {
		roots = []string{}
		if meta.SyncEnabled {
			roots, err = sqliteSyncNowRoots(tx, meta.ComputerID, after.Revision, in.Limit)
			if err != nil {
				return nil, nil, err
			}
		}
		next := sqliteMutationRequestRow{ID: in.RequestID, Value: mutationRequest{Operation: "sync.now", Fingerprint: fp, PendingSync: &syncReservation{Run: &in, RootIDs: roots, SnapshotRevision: after.Revision}}}
		delta, writeErr := sqliteWriteMutationRequest(tx, meta.ComputerID, nil, next)
		if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
			return nil, nil, err
		}
		changedRoots = append(changedRoots, roots...)
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, after, changedRoots, requests)
	return roots, replay, err
}

func sqliteSyncNowComplete(ctx context.Context, a sqliteCaptureAdmission, id, fp string, roots []string) (result SyncRun, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, false, uncertain, id, nil)
			result = SyncRun{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	old, found, err := sqliteSyncNowReceipt(tx, meta, id, fp)
	if err != nil {
		return result, err
	}
	if !found || old.Value.PendingSync == nil || !reflect.DeepEqual(old.Value.PendingSync.RootIDs, roots) {
		return result, failure("state_corrupt")
	}
	if err = sqliteValidateSelectedSync(tx, meta.ComputerID, meta.Revision, roots, []string{id}); err != nil {
		return result, err
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return result, err
	}
	if meta.Revision == "18446744073709551615" {
		return result, failure("validation")
	}
	after := meta
	after.Revision = bump(meta.Revision)
	requests := []string{id}
	for _, root := range roots {
		o, found, readErr := sqliteReadSyncItem(tx, meta.ComputerID, root, meta.Revision)
		if readErr != nil {
			return result, readErr
		}
		if !found {
			return result, failure("state_corrupt")
		}
		if o.RunRequestID == nil {
			continue
		}
		if *o.RunRequestID != id || o.Revision == "18446744073709551615" {
			return result, failure("state_corrupt")
		}
		n := sqliteSyncNowCopyItem(o)
		syncFinishItem(&n)
		if err = sqliteSyncNowWriteItem(tx, meta.ComputerID, o, n, &after.LogicalBytes, &requests); err != nil {
			return result, err
		}
	}
	result, err = sqliteSyncRunResult(tx, meta.ComputerID, id, after.Revision, "complete", roots, false)
	if err != nil {
		return result, err
	}
	next := sqliteMutationRequestRow{ID: id, Value: mutationRequest{Operation: "sync.now", Fingerprint: fp, SyncRun: &result}}
	delta, writeErr := sqliteWriteMutationRequest(tx, meta.ComputerID, &old, next)
	if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
		return result, err
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, after, roots, requests)
	return result, err
}

func (s *Service) sqliteSyncNowSubmit(ctx context.Context, a sqliteCaptureAdmission, budget time.Duration, guard *sqliteSyncRunGuard, request, root string, d SyncDependencies) (posted bool, err error) {
	a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
	o, cfg, enabled, configured, err := sqliteSyncNowSnapshot(ctx, a, request, root)
	if err != nil || !enabled {
		return false, err
	}
	frozen := o.RetryRequestID != nil && o.Plan != nil
	if err = guard.Verify(); err != nil {
		return false, sqliteLinkFailure(err, nil, nil, false, false, request, nil)
	}
	if !configured {
		a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
		return false, sqliteSyncNowBlock(ctx, a, request, o, "configuration_required")
	}
	// Snapshot cleanup has completed before either injected/provider callback.
	provider, prepErr := syncProvider(ctx, d, o.Interval.Attribution.AccountID)
	if prepErr == nil {
		var source string
		source, prepErr = syncPreflight(ctx, provider, o.Interval.Attribution, cfg)
		if prepErr == nil && !frozen {
			plan, buildErr := syncBuildPlan(o, cfg, source)
			if buildErr != nil {
				a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
				return false, sqliteSyncNowBlock(ctx, a, request, o, "representation")
			}
			a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
			o, err = sqliteSyncNowPlan(ctx, a, request, o, plan)
			if err != nil {
				return false, err
			}
		}
	}
	if prepErr != nil {
		a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
		return false, sqliteSyncNowBlock(ctx, a, request, o, syncSafeError(prepErr).Code)
	}
	if o.Plan == nil {
		return false, failure("state_corrupt")
	}
	for index := range o.Plan.Parts {
		if o.Plan.Parts[index].State == "synced" {
			continue
		}
		if err = guard.Verify(); err != nil {
			return posted, sqliteLinkFailure(err, nil, nil, false, posted, request, nil)
		}
		a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
		var claimed bool
		o, claimed, err = sqliteSyncNowClaim(ctx, a, request, o, index, frozen)
		if err != nil || !claimed {
			return posted, err
		}
		if err = s.syncBarrier("sync_after_claim", request); err != nil {
			return posted, err
		}
		if ctx.Err() != nil {
			return posted, requestError(failure("local_write_unknown"), request)
		}
		if err = guard.Verify(); err != nil {
			return posted, sqliteLinkFailure(err, nil, nil, false, true, request, nil)
		}
		// The durable claim's Conn, Tx, cursor and lease are all released. Only
		// the separate run guard spans this single, never-retried remote write.
		posted = true
		response, writeErr := provider.Create(ctx, "/time_entries", syncPayload(o, o.Plan.Parts[index]))
		a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
		o, err = sqliteSyncNowACK(ctx, a, request, o, index, response, writeErr)
		if err != nil {
			return posted, err
		}
		if err = s.syncBarrier("sync_after_part_saved", request); err != nil {
			return posted, err
		}
		if o.Plan.Parts[index].State != "synced" {
			break
		}
	}
	return posted, nil
}

func sqliteSyncNowOwned(tx *sqliteio.Tx, meta sqliteStoreMeta, request, root string) (OutboxItem, error) {
	row, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, request, meta.Revision)
	if err != nil {
		return OutboxItem{}, err
	}
	if !found || row.Value.Operation != "sync.now" || row.Value.PendingSync == nil || row.Value.PendingSync.Run == nil {
		return OutboxItem{}, failure("state_corrupt")
	}
	member := false
	for _, id := range row.Value.PendingSync.RootIDs {
		if id == root {
			member = true
		}
	}
	if !member {
		return OutboxItem{}, failure("state_corrupt")
	}
	o, found, err := sqliteReadSyncItem(tx, meta.ComputerID, root, meta.Revision)
	if err != nil {
		return OutboxItem{}, err
	}
	if !found || o.RunRequestID != nil && *o.RunRequestID != request {
		return OutboxItem{}, failure("state_corrupt")
	}
	return o, nil
}

func sqliteSyncNowSnapshot(ctx context.Context, a sqliteCaptureAdmission, request, root string) (o OutboxItem, cfg SyncConfiguration, enabled, configured bool, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, false, false, request, nil)
			o, cfg, enabled, configured = OutboxItem{}, SyncConfiguration{}, false, false
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Read)
	if err != nil {
		return
	}
	if !found {
		err = failure("state_corrupt")
		return
	}
	o, err = sqliteSyncNowOwned(tx, meta, request, root)
	if err != nil {
		return
	}
	enabled = meta.SyncEnabled
	if o.RetryRequestID != nil && o.Plan != nil {
		cfg, configured = o.Plan.Configuration, true
		return
	}
	cfg, configured, err = sqliteReadSyncConfiguration(tx, o.Interval.Attribution.AccountID, o.Interval.Attribution.UserID)
	return
}

func sqliteSyncNowBlock(ctx context.Context, a sqliteCaptureAdmission, request string, expected OutboxItem, category string) (err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, false, uncertain, request, nil)
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_corrupt")
	}
	o, err := sqliteSyncNowOwned(tx, meta, request, expected.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(o, expected) {
		return failure("revision_conflict")
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return err
	}
	if meta.Revision == "18446744073709551615" || o.Revision == "18446744073709551615" {
		return failure("validation")
	}
	after := meta
	after.Revision = bump(meta.Revision)
	n := sqliteSyncNowCopyItem(o)
	n.State, n.FailureCategory, n.Revision = "needs_attention", syncString(category), bump(o.Revision)
	switch category {
	case "network", "auth", "keychain", "rate_limit", "api":
		n.State = "queued"
	}
	if n.RetryRequestID != nil {
		n.State = "queued"
	}
	requests := []string{request}
	if err = sqliteSyncNowWriteItem(tx, meta.ComputerID, o, n, &after.LogicalBytes, &requests); err != nil {
		return err
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, after, []string{o.ID}, requests)
	return err
}

func sqliteSyncNowPlan(ctx context.Context, a sqliteCaptureAdmission, request string, expected OutboxItem, plan *SyncPlan) (result OutboxItem, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, false, uncertain, request, nil)
			result = OutboxItem{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return result, err
	}
	if !found || plan == nil {
		return result, failure("state_corrupt")
	}
	o, err := sqliteSyncNowOwned(tx, meta, request, expected.ID)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(o, expected) {
		return result, failure("revision_conflict")
	}
	current, found, err := sqliteReadSyncConfiguration(tx, plan.Configuration.AccountID, plan.Configuration.UserID)
	if err != nil {
		return result, err
	}
	if !found || !reflect.DeepEqual(current, plan.Configuration) {
		return result, failure("revision_conflict")
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return result, err
	}
	if meta.Revision == "18446744073709551615" || o.Revision == "18446744073709551615" {
		return result, failure("validation")
	}
	after := meta
	after.Revision = bump(meta.Revision)
	var before *sqliteSyncPlanLocalRow
	if o.Plan != nil {
		old := sqliteSyncPlanLocalRow{IntervalID: o.Interval.ID, CompanySource: o.Plan.CompanySource, Configuration: o.Plan.Configuration}
		before = &old
		for index := len(o.Plan.Parts) - 1; index >= 0; index-- {
			p := o.Plan.Parts[index]
			if len(p.Attempts) != 0 {
				return result, failure("state_corrupt")
			}
			delta, writeErr := sqliteDeleteUnattemptedSyncPart(tx, meta.ComputerID, sqliteSyncNowPart(o.Interval.ID, index, p))
			if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
				return result, err
			}
		}
	}
	delta, writeErr := sqliteWriteSyncPlanLocal(tx, meta.ComputerID, before, sqliteSyncPlanLocalRow{IntervalID: o.Interval.ID, CompanySource: plan.CompanySource, Configuration: plan.Configuration})
	if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
		return result, err
	}
	for index, part := range plan.Parts {
		if len(part.Attempts) != 0 {
			return result, failure("state_corrupt")
		}
		delta, writeErr = sqliteWriteSyncPartLocal(tx, meta.ComputerID, nil, sqliteSyncNowPart(o.Interval.ID, index, part))
		if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
			return result, err
		}
	}
	result = sqliteSyncNowCopyItem(o)
	result.Plan, result.Revision = plan, bump(o.Revision)
	delta, writeErr = sqliteUpdateOutboxLocal(tx, meta.ComputerID, sqliteSyncNowOutbox(o), sqliteSyncNowOutbox(result))
	if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
		return result, err
	}
	requests := []string{request}
	for _, id := range []*string{o.RunRequestID, o.RetryRequestID} {
		if id != nil {
			requests = append(requests, *id)
		}
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, after, []string{o.ID}, requests)
	return result, err
}

func sqliteSyncNowClaim(ctx context.Context, a sqliteCaptureAdmission, request string, expected OutboxItem, index int, frozen bool) (result OutboxItem, claimed bool, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, false, uncertain, request, nil)
			result, claimed = OutboxItem{}, false
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return result, false, err
	}
	if !found {
		return result, false, failure("state_corrupt")
	}
	o, err := sqliteSyncNowOwned(tx, meta, request, expected.ID)
	if err != nil {
		return result, false, err
	}
	if !reflect.DeepEqual(o, expected) {
		return result, false, failure("revision_conflict")
	}
	if !meta.SyncEnabled {
		return o, false, nil
	}
	if o.Plan == nil || index < 0 || index >= len(o.Plan.Parts) || frozen != (o.RetryRequestID != nil) {
		return result, false, failure("state_corrupt")
	}
	if !frozen && index == 0 {
		cfg := o.Plan.Configuration
		current, found, readErr := sqliteReadSyncConfiguration(tx, cfg.AccountID, cfg.UserID)
		if readErr != nil {
			return result, false, readErr
		}
		if !found || !reflect.DeepEqual(current, cfg) {
			return result, false, failure("revision_conflict")
		}
	}
	p := o.Plan.Parts[index]
	if !(p.State == "queued" && len(p.Attempts) == 0) && !(frozen && p.State == "rejected") {
		return result, false, failure("state_corrupt")
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return result, false, err
	}
	if meta.Revision == "18446744073709551615" || o.Revision == "18446744073709551615" {
		return result, false, failure("validation")
	}
	after := meta
	after.Revision = bump(meta.Revision)
	result = sqliteSyncNowCopyItem(o)
	part := &result.Plan.Parts[index]
	part.State, part.FailureCategory = "submitting", nil
	part.Attempts = append(part.Attempts, SyncAttempt{ID: newID(), RequestID: request, Number: strconv.Itoa(len(part.Attempts) + 1), State: "submitting"})
	result.State, result.RunRequestID, result.Revision = "submitting", syncString(request), bump(o.Revision)
	requests := []string{request}
	if err = sqliteSyncNowWriteItem(tx, meta.ComputerID, o, result, &after.LogicalBytes, &requests); err != nil {
		return result, false, err
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, after, []string{o.ID}, requests)
	return result, err == nil, err
}

func sqliteSyncNowACK(ctx context.Context, a sqliteCaptureAdmission, request string, claim OutboxItem, index int, response harvest.Object, writeErr error) (result OutboxItem, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, false, true, request, nil)
			result = OutboxItem{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	o, err := sqliteSyncNowOwned(tx, meta, request, claim.ID)
	if err != nil {
		return result, err
	}
	// This compares the original immutable payload, ordinal and complete attempt
	// identity as well as the submitting owner, never merely the root revision.
	if !reflect.DeepEqual(o, claim) || o.RunRequestID == nil || *o.RunRequestID != request || o.Plan == nil || index < 0 || index >= len(o.Plan.Parts) {
		return result, failure("state_corrupt")
	}
	oldPart := o.Plan.Parts[index]
	if oldPart.State != "submitting" || len(oldPart.Attempts) == 0 || oldPart.Attempts[len(oldPart.Attempts)-1].RequestID != request {
		return result, failure("state_corrupt")
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return result, err
	}
	if meta.Revision == "18446744073709551615" || o.Revision == "18446744073709551615" {
		return result, failure("validation")
	}
	after := meta
	after.Revision = bump(meta.Revision)
	result = sqliteSyncNowCopyItem(o)
	part := &result.Plan.Parts[index]
	if writeErr != nil {
		part.State, part.FailureCategory = "unknown", syncString("remote_write")
		var remote *harvest.Error
		if errors.As(writeErr, &remote) && !remote.Uncertain {
			switch remote.Status {
			case 400, 401, 403, 404, 422, 429:
				part.State = "rejected"
			}
		}
	} else {
		syncApplyResponse(result, part, response)
	}
	attempt := &part.Attempts[len(part.Attempts)-1]
	attempt.State, attempt.EntryID, attempt.FailureCategory = part.State, part.EntryID, part.FailureCategory
	result.Revision = bump(o.Revision)
	requests := []string{request}
	if err = sqliteSyncNowWriteItem(tx, meta.ComputerID, o, result, &after.LogicalBytes, &requests); err != nil {
		return result, err
	}
	_, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, after, []string{o.ID}, requests)
	return result, err
}
