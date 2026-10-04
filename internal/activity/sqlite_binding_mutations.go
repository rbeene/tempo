//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func (s *Service) unlinkSQLite(ctx context.Context, in UnlinkInput) (MutationResult, error) {
	if err := validateBindingMutation(in.BindingID, in.IfRevision, in.RequestID, in.Confirmed); err != nil {
		return MutationResult{}, err
	}
	a, _, err := s.sqliteBindingMutationAdmission(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	p := sqliteBindingMutation{ID: in.RequestID, Operation: "bindings.unlink", Fingerprint: mutationFingerprint("bindings.unlink", in), BindingID: in.BindingID, IfRevision: in.IfRevision}
	receipt, err := sqliteWriteBindingMutation(ctx, a, p, false)
	if err != nil {
		return MutationResult{}, err
	}
	return *receipt.MutationResult, nil
}

func (s *Service) repairBindingSQLite(ctx context.Context, in RepairBindingInput) (BindingResult, error) {
	if err := validateBindingMutation(in.BindingID, in.IfRevision, in.RequestID, in.Confirmed); err != nil {
		return BindingResult{}, err
	}
	if in.Path == "" {
		return BindingResult{}, required("path")
	}
	path, err := lexicalPath(in.Path)
	if err != nil {
		return BindingResult{}, err
	}
	in.Path = path
	a, budget, err := s.sqliteBindingMutationAdmission(ctx)
	if err != nil {
		return BindingResult{}, err
	}
	p := sqliteBindingMutation{ID: in.RequestID, Operation: "bindings.repair", Fingerprint: mutationFingerprint("bindings.repair", in), BindingID: in.BindingID, IfRevision: in.IfRevision}
	before, known, err := sqliteObserveBindingMutation(ctx, a, p)
	if err != nil {
		return BindingResult{}, err
	}
	if !known {
		p.Before = &before
		// No SQL/native owner remains during either discovery. A failed
		// preparation is retained until the writer checks for an exact winner.
		p.Location, p.PreparationError = DiscoverLocation(ctx, in.Path)
		if p.PreparationError == nil && p.Location.Kind != before.Record.Kind {
			p.PreparationError = failure("validation")
		}
		if p.PreparationError == nil {
			p.PreparationError = checkLocation(ctx, in.Path, p.Location)
		}
		// The external discovery phase has ended. Writer admission keeps the
		// caller's deadline and the unchanged per-phase acquisition budget.
		a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
	}
	receipt, err := sqliteWriteBindingMutation(ctx, a, p, known)
	if err != nil {
		return BindingResult{}, err
	}
	return *receipt.BindingResult, nil
}

func (s *Service) sqliteBindingMutationAdmission(ctx context.Context) (sqliteCaptureAdmission, time.Duration, error) {
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

// This preparation belongs only to Unlink/Repair. It carries owned rows and
// values; neither a legacy State nor a discovery callback enters the writer.
type sqliteBindingMutation struct {
	ID, Operation, Fingerprint, BindingID, IfRevision string
	Before                                            *sqliteBindingRow
	Location                                          Location
	PreparationError                                  error
}

// A missing/pristine authority is not initialized by a binding mutation. Every
// returned native owner, including an error owner, belongs to the caller.
func sqliteOpenBindingMutation(ctx context.Context, a sqliteCaptureAdmission, mode sqliteio.Mode) (c *sqliteio.Conn, tx *sqliteio.Tx, meta sqliteStoreMeta, found bool, err error) {
	if mode != sqliteio.Read && mode != sqliteio.Write {
		return nil, nil, meta, false, failure("validation")
	}
	var kind sqliteio.LinkInspection
	c, kind, err = sqliteio.InspectForLink(ctx, a.Directory, a.StateBasename, a.DatabaseBasename, a.AcquireDeadline)
	if err != nil {
		return c, nil, meta, false, err
	}
	if kind == sqliteio.LinkAbsent || kind == sqliteio.LinkPristine {
		if c != nil {
			return c, nil, meta, false, failure("state_corrupt")
		}
		return nil, nil, meta, false, nil
	}
	if kind != sqliteio.LinkWAL || c == nil {
		return c, nil, meta, false, failure("state_corrupt")
	}
	if mode == sqliteio.Write {
		observed := c
		c = nil
		if err = sqliteCaptureClose(observed); err != nil {
			// Close retains any nonterminal owner in its private error. Do not
			// reopen or retry that owner through a second cleanup path.
			return nil, nil, meta, false, errors.Join(failure("local_write_unknown"), err)
		}
		if err = sqliteLinkAdmission(ctx, a.AcquireDeadline); err != nil {
			return nil, nil, meta, false, err
		}
		c, err = sqliteio.Open(ctx, a.Directory, a.DatabaseBasename, sqliteio.Options{AcquireDeadline: a.AcquireDeadline})
		if err != nil {
			return c, nil, meta, false, err
		}
	}
	tx, err = c.Begin(ctx, mode)
	if err != nil {
		return c, tx, meta, false, err
	}
	if mode == sqliteio.Write {
		if err = tx.CheckAuthorityAbsent(a.StateBasename); err != nil {
			return c, tx, meta, false, err
		}
	}
	meta, found, err = sqliteReadLinkSchema(tx, a.StateBasename, a.DatabaseBasename)
	return c, tx, meta, found, err
}

func sqliteObserveBindingMutation(ctx context.Context, a sqliteCaptureAdmission, p sqliteBindingMutation) (binding sqliteBindingRow, known bool, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, false, p.ID, nil)
			binding, known = sqliteBindingRow{}, false
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenBindingMutation(ctx, a, sqliteio.Read)
	if err != nil {
		return binding, false, err
	}
	if !found {
		return binding, false, failure("not_found")
	}
	_, known, err = sqliteBindingMutationReceipt(tx, meta, p)
	if err != nil || known {
		return binding, known, err
	}
	binding, err = sqliteMutableBinding(tx, meta, p.BindingID, p.IfRevision)
	return binding, false, err
}

func sqliteWriteBindingMutation(ctx context.Context, a sqliteCaptureAdmission, p sqliteBindingMutation, known bool) (result mutationRequest, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, uncertain, p.ID, p.PreparationError)
			result = mutationRequest{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenBindingMutation(ctx, a, sqliteio.Write)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("not_found")
	}
	row, found, err := sqliteBindingMutationReceipt(tx, meta, p)
	if err != nil {
		return result, err
	}
	if known && !found {
		return result, failure("state_corrupt")
	}
	known = known || found
	if found {
		result = row.Value
		err = sqliteFenceBindingMutation(tx, meta)
	} else if p.PreparationError != nil {
		err = p.PreparationError
	} else {
		result, err = sqliteApplyBindingMutation(tx, meta, p)
	}
	if err != nil {
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
