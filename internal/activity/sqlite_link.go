//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/privatefs"
)

// linkSQLite owns Link's fresh-store SQLite path.
func (s *Service) linkSQLite(ctx context.Context, in LinkInput, d LinkDependencies) (BindingResult, error) {
	if err := validateLinkInput(in); err != nil {
		return BindingResult{}, err
	}
	path, err := lexicalPath(in.Path)
	if err != nil {
		return BindingResult{}, err
	}
	in.Path = path
	if s == nil || s.store == nil {
		return BindingResult{}, failure("validation")
	}
	budget := s.store.timeout
	if budget == 0 {
		budget = 250 * time.Millisecond
	}
	if budget < 0 || budget > time.Second {
		return BindingResult{}, failure("validation")
	}
	directory, authority, database, err := sqliteLocation(s.store.path)
	if err != nil {
		return BindingResult{}, err
	}
	p := sqliteLinkPrepared{Input: in, Fingerprint: mutationFingerprint("bindings.link", in)}
	first, err := sqliteLinkObserve(ctx, directory, authority, database, sqliteLinkDeadline(ctx, budget), p, nil)
	if err != nil {
		return BindingResult{}, err
	}
	if first.receipt {
		return sqliteLinkWrite(ctx, directory, authority, database, sqliteLinkDeadline(ctx, budget), p, false, true, true)
	}

	// Every native read owner is already closed. Discovery and provider reads
	// may themselves acquire the store's native writer or initialization guard.
	p.Location, p.PreparationError = DiscoverLocation(ctx, in.Path)
	var saved *BindingSnapshot
	if p.PreparationError == nil && first.kind == sqliteio.LinkWAL {
		observed, readErr := sqliteLinkObserve(ctx, directory, authority, database, sqliteLinkDeadline(ctx, budget), p, &p.Location)
		if readErr != nil {
			return BindingResult{}, readErr
		}
		if observed.kind != sqliteio.LinkWAL {
			return BindingResult{}, failure("state_corrupt")
		}
		if observed.receipt {
			return sqliteLinkWrite(ctx, directory, authority, database, sqliteLinkDeadline(ctx, budget), p, false, true, true)
		}
		saved = observed.binding
	}
	if p.PreparationError == nil && in.IfRevision != "" && (saved == nil || saved.Revision != in.IfRevision) {
		p.PreparationError = failure("revision_conflict")
	}
	if p.PreparationError == nil {
		p.Attribution, p.PreparationError = assignedAttribution(ctx, in, d, saved)
	}
	if p.PreparationError == nil {
		p.PreparationError = checkLocation(ctx, in.Path, p.Location)
	}
	if p.PreparationError != nil {
		// One noncreating final observation can discover a concurrent winner.
		// A failed observation is not absence and cannot be hidden by the
		// preparation error. The WAL writer then checks the request again.
		observed, readErr := sqliteLinkObserve(ctx, directory, authority, database, sqliteLinkDeadline(ctx, budget), p, nil)
		if readErr != nil {
			return BindingResult{}, readErr
		}
		if observed.kind != sqliteio.LinkWAL {
			return BindingResult{}, p.PreparationError
		}
		return sqliteLinkWrite(ctx, directory, authority, database, sqliteLinkDeadline(ctx, budget), p, false, observed.receipt, observed.receipt)
	}
	return sqliteLinkWrite(ctx, directory, authority, database, sqliteLinkDeadline(ctx, budget), p, first.kind != sqliteio.LinkWAL, false, false)
}

func sqliteLinkDeadline(ctx context.Context, budget time.Duration) time.Time {
	deadline := time.Now().Add(budget)
	if caller, ok := ctx.Deadline(); ok && caller.Before(deadline) {
		deadline = caller
	}
	return deadline
}

type sqliteLinkObservation struct {
	kind    sqliteio.LinkInspection
	receipt bool
	binding *BindingSnapshot
}

// A Read only establishes receipt identity, never durability. It owns exactly
// one read-only inspection connection, one Read, and their terminal cleanup.
func sqliteLinkObserve(ctx context.Context, directory, authority, database string, deadline time.Time, p sqliteLinkPrepared, location *Location) (result sqliteLinkObservation, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	known := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, false, p.Input.RequestID, nil)
			result = sqliteLinkObservation{}
		}
	}()
	var kind sqliteio.LinkInspection
	c, kind, err = sqliteio.InspectForLink(ctx, directory, authority, database, deadline)
	if err != nil {
		// A nonnil error owner is solely for the checked ordinary cleanup above.
		return sqliteLinkObservation{}, err
	}
	if kind == sqliteio.LinkAbsent || kind == sqliteio.LinkPristine {
		if c != nil {
			return sqliteLinkObservation{}, failure("state_corrupt")
		}
		return sqliteLinkObservation{kind: kind}, nil
	}
	if kind != sqliteio.LinkWAL || c == nil {
		return sqliteLinkObservation{}, failure("state_corrupt")
	}
	tx, err = c.Begin(ctx, sqliteio.Read)
	if err != nil {
		return sqliteLinkObservation{}, err
	}
	meta, current, err := sqliteReadLinkSchema(tx, authority, database)
	if err != nil {
		return sqliteLinkObservation{}, err
	}
	result.kind = kind
	if !current {
		return result, nil
	}
	_, known, err = sqliteLinkReceipt(tx, meta, p.Input.RequestID, p.Fingerprint)
	if err != nil {
		return sqliteLinkObservation{}, err
	}
	result.receipt = known
	if !known && location != nil {
		binding, found, readErr := sqliteReadBindingLocation(tx, meta.ComputerID, location.Kind, location.Locator)
		if readErr != nil {
			return sqliteLinkObservation{}, readErr
		}
		if found {
			snapshot := binding.Snapshot
			result.binding = &snapshot
		}
	}
	return result, nil
}

func sqliteLinkWrite(ctx context.Context, directory, authority, database string, deadline time.Time, p sqliteLinkPrepared, initialize, known, replayOnly bool) (result BindingResult, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, uncertain, p.Input.RequestID, p.PreparationError)
			result = BindingResult{}
		}
	}()
	if initialize {
		if err = sqliteLinkAdmission(ctx, deadline); err != nil {
			return BindingResult{}, err
		}
		root, _, openErr := privatefs.OpenDirectory(directory, true)
		var closeErr error
		if root != nil {
			closeErr = root.Close()
		}
		if openErr != nil || closeErr != nil {
			uncertain = closeErr != nil || errors.Is(openErr, privatefs.ErrDurability)
			return BindingResult{}, errors.Join(openErr, closeErr)
		}
		if err = sqliteLinkAdmission(ctx, deadline); err != nil {
			return BindingResult{}, err
		}
		c, err = sqliteio.OpenForInitialLink(ctx, directory, authority, database, deadline)
	} else {
		c, err = sqliteio.Open(ctx, directory, database, sqliteio.Options{AcquireDeadline: deadline})
	}
	if err != nil {
		return BindingResult{}, err
	}
	if c == nil {
		return BindingResult{}, failure("state_corrupt")
	}
	tx, err = c.Begin(ctx, sqliteio.Write)
	if err != nil {
		return BindingResult{}, err
	}
	// This is the first application observation after the actual Write Begin,
	// including replay and a preparation-failure winner lookup after a wait.
	if err = tx.CheckAuthorityAbsent(authority); err != nil {
		return BindingResult{}, err
	}
	if replayOnly {
		var meta sqliteStoreMeta
		var current, found bool
		meta, current, err = sqliteReadLinkSchema(tx, authority, database)
		if err == nil && !current {
			err = failure("state_corrupt")
		}
		if err == nil {
			result, found, err = sqliteReplayLink(tx, meta, p.Input.RequestID, p.Fingerprint)
			if err == nil && !found {
				err = failure("state_corrupt")
			}
		}
	} else {
		var established bool
		result, established, err = sqliteLinkUnit(tx, authority, database, p)
		known = known || established
	}
	if err != nil {
		return BindingResult{}, err
	}
	outcome, commitErr := tx.Commit()
	if outcome != sqliteio.Committed || commitErr != nil {
		uncertain = outcome == sqliteio.Unknown || outcome == sqliteio.Committed
		if commitErr == nil {
			commitErr = failure("state_corrupt")
		}
		return BindingResult{}, commitErr
	}
	if err = c.CloseDurably(ctx); err != nil {
		uncertain = true
		return BindingResult{}, err
	}
	return result, nil
}

func sqliteLinkAdmission(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !time.Now().Before(deadline) {
		return sqliteio.ErrBusy
	}
	return nil
}

// CloseChecked is a separately reviewed native prerequisite. Its boolean is
// caller-ownership evidence, not proof that a failed OS cleanup succeeded.
// Only a nonterminal result allows one ordinary same-owner cleanup retry.
func sqliteLinkCleanup(tx *sqliteio.Tx, c *sqliteio.Conn) (error, *sqliteio.Conn) {
	var cleanup error
	if tx != nil {
		cleanup = tx.Rollback()
	}
	if c == nil {
		return cleanup, nil
	}
	terminal, closeErr := c.CloseChecked(context.Background())
	cleanup = errors.Join(cleanup, closeErr)
	if !terminal {
		var retryErr error
		terminal, retryErr = c.CloseChecked(context.Background())
		cleanup = errors.Join(cleanup, retryErr)
	}
	if !terminal {
		return cleanup, c
	}
	return cleanup, nil
}

// The exceptional owner stays reachable through the returned error. No global
// owner registry, finalizer, detached retry or false release claim is added.
// Evidence is retained privately; Error/Unwrap expose only the public safe
// outcome, after request_id and uncertainty classification are complete.
type sqliteLinkFailureError struct {
	public   error
	evidence error
	owner    *sqliteio.Conn
}

func (e *sqliteLinkFailureError) Error() string { return e.public.Error() }
func (e *sqliteLinkFailureError) Unwrap() error { return e.public }

// Preserve only caller cancellation identity after a definite, owner-free
// busy outcome. Private evidence stays outside the public unwrap chain.
func (e *sqliteLinkFailureError) Is(target error) bool {
	if target != context.Canceled && target != context.DeadlineExceeded {
		return false
	}
	public, ok := e.public.(*Error)
	return ok && public != nil && public.Code == "state_busy" && !public.Uncertain && e.owner == nil && errors.Is(e.evidence, target)
}

func sqliteLinkFailure(cause, cleanup error, owner *sqliteio.Conn, known, uncertain bool, requestID string, preparation error) error {
	var domain *Error
	var native *sqliteio.Error
	errors.As(cause, &domain)
	errors.As(cause, &native)
	var public error
	switch {
	case known || uncertain || cleanup != nil || owner != nil || sqliteLinkHasCleanup(cause) || domain != nil && domain.Code == "local_write_unknown":
		public = requestError(failure("local_write_unknown"), requestID)
	case preparation != nil && errors.Is(cause, preparation):
		// Preserve the already selected provider/preparation error exactly when
		// no historical receipt or uncertain cleanup superseded it.
		public = preparation
	case errors.Is(cause, context.Canceled), errors.Is(cause, context.DeadlineExceeded), errors.Is(cause, sqliteio.ErrBusy):
		public = failure("state_busy")
	case native != nil && (native.Category == sqliteio.Busy || native.Category == sqliteio.Canceled):
		public = failure("state_busy")
	case native != nil && native.Phase == sqliteio.Admission && native.Category == sqliteio.Unsafe && native.Cause == os.ErrExist:
		public = &Error{Code: "state_path_in_use", Message: "The configured state path already exists; choose a new absolute TEMPO_STATE path."}
	case native != nil && (native.Category == sqliteio.Full || native.Cause == syscall.ENOSPC):
		public = sqliteLinkCapacityError("database_capacity")
	case domain != nil:
		public = domain
	default:
		public = failure("state_corrupt")
	}
	return &sqliteLinkFailureError{public: public, evidence: errors.Join(cause, cleanup), owner: owner}
}

func sqliteLinkHasCleanup(err error) bool {
	if err == nil {
		return false
	}
	if native, ok := err.(*sqliteio.Error); ok && native.Cleanup != nil {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if sqliteLinkHasCleanup(child) {
				return true
			}
		}
	} else if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return sqliteLinkHasCleanup(wrapped.Unwrap())
	}
	return false
}
