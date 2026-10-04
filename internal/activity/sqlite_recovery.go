//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"sort"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Exactly two recovery mutations share receipt/admission ownership. Neither
// variant accepts an apply callback or reconstructs a legacy state.
type sqliteRecoveryChange struct {
	ID, Operation, Fingerprint string
	Resolve                    *ResolveInput
	Interrupt                  *InterruptInput
}
type sqliteRecoveryObservation struct {
	Computer    string
	Known       bool
	Target      *sqliteRecoveryTarget
	Actor       *sqliteActorLocalRow
	Clock       []sqliteCaptureClockActor
	NeedsSample bool
	Sample      *sqliteCaptureSample
}

func (s *Service) resolveSQLite(ctx context.Context, in ResolveInput) (MutationResult, error) {
	owned := in
	if in.End != nil {
		at := *in.End
		owned.End = &at
	}
	return s.recoverySQLite(ctx, sqliteRecoveryChange{ID: in.RequestID, Operation: "activity.resolve", Fingerprint: mutationFingerprint("activity.resolve", in), Resolve: &owned})
}
func (s *Service) interruptSQLite(ctx context.Context, in InterruptInput) (MutationResult, error) {
	return s.recoverySQLite(ctx, sqliteRecoveryChange{ID: in.RequestID, Operation: "activity.interrupt", Fingerprint: mutationFingerprint("activity.interrupt", in), Interrupt: &in})
}
func (s *Service) recoverySQLite(ctx context.Context, change sqliteRecoveryChange) (MutationResult, error) {
	if !validUUID(change.ID) {
		return MutationResult{}, failure("validation")
	}
	admission, _, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	var prepared *sqliteRecoveryObservation
	if !s.nativeCaptureClock {
		observed, e := sqliteRecoveryObserve(ctx, admission, change)
		if e != nil {
			return MutationResult{}, e
		}
		prepared = &observed
		if observed.NeedsSample {
			value, sampleErr := s.sample()
			prepared.Sample = &sqliteCaptureSample{Value: sqliteCaptureCopySample(value), Unavailable: sampleErr != nil}
		}
	}
	receipt, err := sqliteRecoveryWrite(ctx, admission, change, prepared)
	if err != nil {
		return MutationResult{}, err
	}
	return recoveryReceipt(receipt)
}

func sqliteRecoveryReceipt(tx *sqliteio.Tx, meta sqliteStoreMeta, change sqliteRecoveryChange) (mutationRequest, bool, error) {
	row, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, change.ID, meta.Revision)
	if err != nil || !found {
		return mutationRequest{}, false, err
	}
	if row.Value.Operation != change.Operation || row.Value.Fingerprint != change.Fingerprint {
		return mutationRequest{}, false, failure("request_conflict")
	}
	if row.Value.MutationResult == nil && row.Value.Error == nil {
		return mutationRequest{}, false, failure("state_corrupt")
	}
	if change.Resolve != nil && row.Value.MutationResult != nil {
		if err = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, meta.Revision, sqliteDependencySelection{ResolveRequestIDs: []string{change.ID}}); err != nil {
			return row.Value, true, err
		}
	}
	return row.Value, true, nil
}

func sqliteRecoveryActorByID(tx *sqliteio.Tx, meta sqliteStoreMeta, id string) (result *sqliteActorLocalRow, err error) {
	stmt, err := tx.Prepare("SELECT actor_key,generation FROM actors WHERE id=?", sqliteio.Text(id))
	if err != nil {
		return nil, err
	}
	var ref ActorRef
	var found bool
	func() {
		defer func() { err = sqliteCloseMetaStatement(stmt, err) }()
		found, err = stmt.Step()
		if err != nil || !found {
			return
		}
		if stmt.ColumnCount() != 2 {
			err = failure("state_corrupt")
			return
		}
		ref, err = sqliteDependencyStoredActor(tx, stmt, meta.ComputerID)
		if err != nil {
			return
		}
		more, e := stmt.Step()
		if e != nil {
			err = e
		} else if more {
			err = failure("state_corrupt")
		}
	}()
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, failure("actor_not_found")
	}
	result, err = sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, ref.Key)
	if err != nil {
		return nil, err
	}
	if result == nil || result.ID != id || result.Ref != ref {
		return nil, failure("state_corrupt")
	}
	return result, nil
}

func sqliteRecoveryObserveUnit(tx *sqliteio.Tx, meta sqliteStoreMeta, change sqliteRecoveryChange) (result sqliteRecoveryObservation, err error) {
	result.Computer = meta.ComputerID
	_, result.Known, err = sqliteRecoveryReceipt(tx, meta, change)
	if err != nil || result.Known {
		return result, err
	}
	switch {
	case change.Resolve != nil:
		target, e := sqliteRecoveryReadTarget(tx, meta, change.Resolve.UncertaintyID)
		if e != nil {
			return result, e
		}
		if target.Uncertainty.Revision != change.Resolve.IfRevision {
			return result, failure("revision_conflict")
		}
		result.Target = &target
		result.NeedsSample = !change.Resolve.DiscardTail
	case change.Interrupt != nil:
		actor, e := sqliteRecoveryActorByID(tx, meta, change.Interrupt.ActorID)
		if e != nil {
			return result, e
		}
		if actor.Ref.Generation != change.Interrupt.Generation || actor.Revision != change.Interrupt.IfRevision {
			return result, failure("revision_conflict")
		}
		if sqliteCaptureTerminal(*actor) {
			return result, failure("invalid_transition")
		}
		result.Actor = actor
		result.NeedsSample = actor.State == "working"
	default:
		return result, failure("validation")
	}
	if result.NeedsSample {
		result.Clock, err = sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
	}
	return result, err
}

func sqliteRecoveryObserve(ctx context.Context, a sqliteCaptureAdmission, change sqliteRecoveryChange) (result sqliteRecoveryObservation, err error) {
	c, tx, meta, found, err := sqliteRecoveryOpenRead(ctx, a)
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, result.Known, false, change.ID, nil)
			result = sqliteRecoveryObservation{}
		}
	}()
	if err != nil {
		return result, err
	}
	if !found {
		return result, requiredRecovery("binding")
	}
	return sqliteRecoveryObserveUnit(tx, meta, change)
}

func sqliteRecoveryWrite(ctx context.Context, a sqliteCaptureAdmission, change sqliteRecoveryChange, prepared *sqliteRecoveryObservation) (result mutationRequest, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	known, uncertain := prepared != nil && prepared.Known, false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, uncertain, change.ID, nil)
			result = mutationRequest{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return result, err
	}
	if !found {
		return result, requiredRecovery("binding")
	}
	result, found, err = sqliteRecoveryReceipt(tx, meta, change)
	wasKnown := known
	known = known || found
	if err != nil {
		return result, err
	}
	if wasKnown && !found {
		return result, failure("state_corrupt")
	}
	var observed sqliteRecoveryObservation
	if !found {
		observed, err = sqliteRecoveryObserveUnit(tx, meta, change)
		if err != nil {
			return result, err
		}
		if prepared != nil {
			if prepared.Computer != meta.ComputerID || prepared.NeedsSample != observed.NeedsSample || !reflect.DeepEqual(prepared.Target, observed.Target) || !reflect.DeepEqual(prepared.Actor, observed.Actor) || !reflect.DeepEqual(prepared.Clock, observed.Clock) {
				return result, failure("state_busy")
			}
		}
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return result, err
	}
	after := meta
	if !found {
		if meta.Revision == "18446744073709551615" {
			return result, failure("validation")
		}
		after.Revision = bump(meta.Revision)
		m := sqliteCaptureMutation{tx: tx, meta: meta, nextRevision: after.Revision}
		var sample *ClockSample
		var clockErr error
		if observed.NeedsSample {
			clock := sqliteCaptureClock{Mode: sqliteCaptureNative, Site: sqliteCaptureClockReduce}
			if prepared != nil {
				clock.Mode = sqliteCapturePrepared
				clock.Prepared = prepared.Sample
			}
			value, e := clock.take(sqliteCaptureClockReduce)
			clockErr = e
			sample = &value
		}
		var mutation MutationResult
		var operationErr *Error
		if change.Resolve != nil {
			mutation, operationErr, err = sqliteRecoveryApplyResolve(&m, *change.Resolve, *observed.Target, sample, clockErr)
		} else {
			mutation, operationErr, err = sqliteRecoveryApplyInterrupt(&m, *change.Interrupt, *observed.Actor, sample, clockErr)
		}
		if err != nil {
			return result, err
		}
		if operationErr != nil && !m.transition.Changed {
			return result, operationErr
		}
		result = mutationRequest{Operation: change.Operation, Fingerprint: change.Fingerprint}
		if operationErr != nil {
			result.Error = failure(operationErr.Code)
		} else {
			mutation.ContractVersion = 1
			mutation.SnapshotRevision = after.Revision
			mutation.RequestID = change.ID
			if mutation.AffectedIDs == nil {
				mutation.AffectedIDs = []string{}
			}
			sort.Strings(mutation.AffectedIDs)
			result.MutationResult = &mutation
		}
		n, e := sqliteWriteMutationRequest(tx, meta.ComputerID, nil, sqliteMutationRequestRow{ID: change.ID, Value: result})
		if err = m.add(n, e); err != nil {
			return result, err
		}
		if change.Resolve != nil && operationErr == nil {
			m.transition.Dependencies.ChangedResolveRequestIDs = append(m.transition.Dependencies.ChangedResolveRequestIDs, change.ID)
		}
		if err = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, after.Revision, m.transition.Dependencies); err != nil {
			return result, err
		}
		if err = sqliteValidateSelectedFinalization(tx, meta.ComputerID, after.Revision, m.transition.Finalization); err != nil {
			return result, err
		}
		if err = sqliteFinalizationAdd(&after.LogicalBytes, m.transition.Delta, nil); err != nil {
			return result, err
		}
	}
	after.DurabilityNonce, err = sqliteNextNonce(meta.DurabilityNonce[:])
	if err == nil {
		err = sqliteUpdateMeta(tx, meta, after)
	}
	if err != nil {
		return result, err
	}
	checked, e := sqliteReadMeta(tx, a.StateBasename, a.DatabaseBasename)
	if e != nil {
		return result, e
	}
	if !reflect.DeepEqual(checked, after) {
		return result, failure("state_corrupt")
	}
	result, found, err = sqliteRecoveryReceipt(tx, after, change)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	outcome, commitErr := tx.Commit()
	if commitErr != nil || outcome != sqliteio.Committed {
		uncertain = outcome == sqliteio.Unknown || outcome == sqliteio.Committed
		if commitErr == nil {
			commitErr = failure("state_corrupt")
		}
		return result, commitErr
	}
	if err = c.CloseDurably(ctx); err != nil {
		uncertain = true
		return result, err
	}
	return result, nil
}

func sqliteRecoveryDomain(err error) (*Error, error) {
	if err == nil {
		return nil, nil
	}
	var domain *Error
	if errors.As(err, &domain) {
		switch domain.Code {
		case "clock_unavailable", "clock_conflict", "recovery_bounds", "attribution_conflict":
			return domain, nil
		}
	}
	return nil, err
}
