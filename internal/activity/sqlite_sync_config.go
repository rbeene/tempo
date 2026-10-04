//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"reflect"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func (s *Service) syncConfigureSQLite(ctx context.Context, in SyncConfigureInput, d SyncDependencies) (SyncConfigurationResult, error) {
	if err := validateSyncConfig(in); err != nil {
		return SyncConfigurationResult{}, err
	}
	if ctx == nil {
		return SyncConfigurationResult{}, failure("validation")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	a, budget, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return SyncConfigurationResult{}, err
	}
	change := sqliteSyncConfigChange{ID: in.RequestID, Operation: "sync.configure", Fingerprint: mutationFingerprint("sync.configure", in), Input: &in}
	found, known, err := sqliteSyncConfigObserve(ctx, a, change)
	if err != nil {
		return SyncConfigurationResult{}, err
	}
	if !found {
		return SyncConfigurationResult{}, syncRequired("binding")
	}
	if !known {
		// The read owner is conclusively released before either provider callback.
		provider, err := syncProvider(ctx, d, in.AccountID)
		if err != nil {
			return SyncConfigurationResult{}, err
		}
		user, err := syncIdentity(ctx, provider, in.AccountID)
		if err != nil {
			return SyncConfigurationResult{}, err
		}
		change.UserID = bindingID(user)
		if in.UserID != "" && in.UserID != change.UserID {
			return SyncConfigurationResult{}, failure("identity_conflict")
		}
		// External I/O ends the first phase. Its writer gets one new admission
		// deadline, still bounded by the original two-minute action context.
		a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
	}
	receipt, err := sqliteSyncConfigWrite(ctx, a, change, known)
	if err != nil {
		return SyncConfigurationResult{}, err
	}
	return *receipt.SyncConfigurationResult, nil
}

func (s *Service) syncControlSQLite(ctx context.Context, requestID string, enabled bool) (MutationResult, error) {
	if !validUUID(requestID) {
		return MutationResult{}, failure("validation")
	}
	a, _, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	op := "sync.pause"
	if enabled {
		op = "sync.resume"
	}
	change := sqliteSyncConfigChange{ID: requestID, Operation: op, Fingerprint: mutationFingerprint(op, requestID), Enabled: enabled}
	receipt, err := sqliteSyncConfigWrite(ctx, a, change, false)
	if err != nil {
		return MutationResult{}, err
	}
	return *receipt.MutationResult, nil
}

func (s *Service) sqliteSyncConfigAdmission(ctx context.Context) (sqliteCaptureAdmission, time.Duration, error) {
	if ctx == nil || s == nil || s.store == nil {
		return sqliteCaptureAdmission{}, 0, failure("validation")
	}
	budget := s.store.timeout
	if budget == 0 {
		budget = 250 * time.Millisecond
	}
	if budget < 0 || budget > time.Second {
		return sqliteCaptureAdmission{}, 0, failure("validation")
	}
	directory, authority, database, err := sqliteLocation(s.store.path)
	if err != nil {
		return sqliteCaptureAdmission{}, 0, err
	}
	return sqliteCaptureAdmission{Directory: directory, StateBasename: authority, DatabaseBasename: database, AcquireDeadline: sqliteLinkDeadline(ctx, budget)}, budget, nil
}

// This finite unit covers configuration and the enabled flag only. It contains
// no callback, provider, sync guard or upload operation.
type sqliteSyncConfigChange struct {
	ID, Operation, Fingerprint string
	Input                      *SyncConfigureInput
	UserID                     string
	Enabled                    bool
}

func sqliteSyncConfigReceipt(tx *sqliteio.Tx, meta sqliteStoreMeta, change sqliteSyncConfigChange) (mutationRequest, bool, error) {
	row, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, change.ID, meta.Revision)
	if err != nil || !found {
		return mutationRequest{}, false, err
	}
	if row.Value.Operation != change.Operation || row.Value.Fingerprint != change.Fingerprint {
		return mutationRequest{}, false, failure("request_conflict")
	}
	if change.Operation == "sync.configure" {
		if row.Value.SyncConfigurationResult == nil {
			return mutationRequest{}, false, failure("state_corrupt")
		}
	} else if (change.Operation != "sync.pause" && change.Operation != "sync.resume") || row.Value.MutationResult == nil {
		return mutationRequest{}, false, failure("state_corrupt")
	}
	return row.Value, true, nil
}

func sqliteSyncConfigObserve(ctx context.Context, a sqliteCaptureAdmission, change sqliteSyncConfigChange) (found, known bool, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, false, change.ID, nil)
			found, known = false, false
		}
	}()
	var kind sqliteio.LinkInspection
	c, kind, err = sqliteio.InspectForLink(ctx, a.Directory, a.StateBasename, a.DatabaseBasename, a.AcquireDeadline)
	if err != nil {
		return false, false, err
	}
	if kind == sqliteio.LinkAbsent || kind == sqliteio.LinkPristine {
		if c != nil {
			return false, false, failure("state_corrupt")
		}
		return false, false, nil
	}
	if kind != sqliteio.LinkWAL || c == nil {
		return false, false, failure("state_corrupt")
	}
	tx, err = c.Begin(ctx, sqliteio.Read)
	if err != nil {
		return false, false, err
	}
	var meta sqliteStoreMeta
	meta, found, err = sqliteReadLinkSchema(tx, a.StateBasename, a.DatabaseBasename)
	if err != nil || !found {
		return found, false, err
	}
	_, known, err = sqliteSyncConfigReceipt(tx, meta, change)
	if err == nil && known {
		err = sqliteValidateSelectedSync(tx, meta.ComputerID, meta.Revision, nil, []string{change.ID})
	}
	return found, known, err
}

func sqliteSyncConfigWrite(ctx context.Context, a sqliteCaptureAdmission, change sqliteSyncConfigChange, known bool) (result mutationRequest, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, uncertain, change.ID, nil)
			result = mutationRequest{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	// Shared admission qualifies the physical WAL without creating authority,
	// then checks P absence immediately after the actual Write BEGIN.
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return result, err
	}
	if !found {
		return result, syncRequired("binding")
	}
	result, found, err = sqliteSyncConfigReceipt(tx, meta, change)
	if err != nil {
		return result, err
	}
	if known && !found {
		return result, failure("state_corrupt")
	}
	known = known || found
	if found {
		if err = sqliteValidateSelectedSync(tx, meta.ComputerID, meta.Revision, nil, []string{change.ID}); err != nil {
			return result, err
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
		result = mutationRequest{Operation: change.Operation, Fingerprint: change.Fingerprint}
		switch change.Operation {
		case "sync.configure":
			if change.Input == nil {
				return result, failure("validation")
			}
			in := *change.Input
			previous, exists, readErr := sqliteReadSyncConfiguration(tx, in.AccountID, change.UserID)
			if readErr != nil {
				return result, readErr
			}
			revision := "0"
			var before *SyncConfiguration
			if exists {
				revision, before = previous.Revision, &previous
			}
			if revision != in.IfRevision {
				return result, failure("revision_conflict")
			}
			if revision == "18446744073709551615" {
				return result, failure("validation")
			}
			configuration := SyncConfiguration{AccountID: in.AccountID, UserID: change.UserID, Revision: bump(revision), Mode: in.Mode, DurationPolicy: in.DurationPolicy, PolicyVersion: in.DurationPolicy + "-v1", Declared: true, DeclaredAt: time.Now().UTC(), Source: "user_declared"}
			if in.Clock != "" {
				value := in.Clock
				configuration.Clock = &value
			}
			delta, writeErr := sqliteWriteSyncConfiguration(tx, before, configuration)
			if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
				return result, err
			}
			result.SyncConfigurationResult = &SyncConfigurationResult{ContractVersion: 1, SnapshotRevision: after.Revision, RequestID: change.ID, Changed: true, Configuration: configuration}
		case "sync.pause", "sync.resume":
			after.SyncEnabled = change.Enabled
			result.MutationResult = &MutationResult{ContractVersion: 1, SnapshotRevision: after.Revision, RequestID: change.ID, Changed: meta.SyncEnabled != change.Enabled, AffectedIDs: []string{}}
		default:
			return result, failure("validation")
		}
		row := sqliteMutationRequestRow{ID: change.ID, Value: result}
		delta, writeErr := sqliteWriteMutationRequest(tx, meta.ComputerID, nil, row)
		if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
			return result, err
		}
		if err = sqliteValidateSelectedSync(tx, meta.ComputerID, after.Revision, nil, []string{change.ID}); err != nil {
			return result, err
		}
	}
	// A replay advances only this nonce. A new no-op control request still
	// retains its own receipt and increments the public metadata revision.
	after.DurabilityNonce, err = sqliteNextNonce(meta.DurabilityNonce[:])
	if err == nil {
		err = sqliteUpdateMeta(tx, meta, after)
	}
	if err != nil {
		return result, err
	}
	observed, err := sqliteReadMeta(tx, a.StateBasename, a.DatabaseBasename)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(observed, after) {
		return result, failure("state_corrupt")
	}
	// Return the materialized typed receipt, including owned optional values.
	result, found, err = sqliteSyncConfigReceipt(tx, after, change)
	if err != nil || !found {
		if err == nil {
			err = failure("state_corrupt")
		}
		return result, err
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
