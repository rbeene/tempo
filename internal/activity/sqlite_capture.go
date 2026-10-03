//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteCaptureAdmission struct {
	Directory, StateBasename, DatabaseBasename string
	AcquireDeadline                            time.Time
}

type sqliteCaptureBranch uint8

const (
	sqliteCaptureNoSample sqliteCaptureBranch = iota
	sqliteCaptureGapSample
	sqliteCaptureExistingSample
	sqliteCaptureNewSample
)

type sqliteCaptureBindingLookup struct {
	Kind, Locator string
	Row           *sqliteBindingRow // nil records a successful exact lookup with no row
}

type sqliteCaptureBinding struct {
	Needed          bool
	Snapshot        *BindingSnapshot // nil means no admitted binding
	ErrorCode       string           // "", "validation", "binding_unavailable", "attribution_conflict"
	Conflict        *Attribution     // only attribution_conflict; paired with Snapshot.Attribution
	ExplicitChecked bool
	Explicit        *sqliteBindingRow            // nil is an observed absence for e.BindingID
	Locations       []sqliteCaptureBindingLookup // discovery lookup order, stop at first match
	ParentChecked   bool
	Parent          *sqliteActorLocalRow // nil is an observed absence for e.Parent.Key
	TimerChecked    bool
	Timer           []sqliteBindingRow // all active rows for selected snapshot's timer, ID sorted
}

type sqliteCaptureWaitTurn struct {
	Row     sqliteHostTurnRow
	Pending map[string]hostTool // complete phase=pre subset, owned values
}

type sqliteCaptureClockActor struct {
	Actor      sqliteActorLocalRow
	Segment    *sqliteSegmentLocalRow  // exact Actor.SegmentID, when present
	Epoch      *sqliteEpochRow         // exact Segment.EpochID, when present
	WaitTurns  []sqliteCaptureWaitTurn // only Claude wait_user, all exact-Ref turns
	LatestWait *sqliteHostReceiptRow   // only when the actual pending-wait predicate is true
}

type sqliteCapturePreparation struct {
	ComputerID    string
	RefusalCode   string                 // "" or "state_busy": selected before facts changed in read B
	Receipt       *sqliteEventReceiptRow // initial exact-key receipt; nil means absent
	EventIDKey    *string                // initial exact raw EventID lookup, when reached
	TargetChecked bool
	Target        *sqliteActorLocalRow // full owned current head; nil means absent
	Branch        sqliteCaptureBranch
	Binding       sqliteCaptureBinding
	ClockActors   []sqliteCaptureClockActor // prepared mode only; sorted by actorKey
}

type sqliteEventTransition struct {
	Result         EventResult
	Changed        bool
	OperationError *Error
	Delta          int64
	Dependencies   sqliteDependencySelection
	Finalization   sqliteFinalizationSelection
}

// Admission uses one absolute budget across preliminary reads, callbacks and BEGIN.
func sqliteCaptureDeadline(ctx context.Context, a sqliteCaptureAdmission) error {
	if ctx.Err() != nil || a.AcquireDeadline.IsZero() || !time.Now().Before(a.AcquireDeadline) {
		return failure("state_busy")
	}
	return nil
}

// This exceptional owner is retained privately, never serialized or logged.
// It survives a bounded cleanup refusal without a detached retry or registry.
type sqliteCaptureCleanupError struct {
	public   *Error
	evidence error
	owner    *sqliteio.Conn
}

func (e *sqliteCaptureCleanupError) Error() string { return e.public.Error() }
func (e *sqliteCaptureCleanupError) Unwrap() error { return e.public }

// CloseChecked's terminal result proves caller ownership ended, not successful
// durability. Only false permits one same-owner retry; all errors survive.
// Persistent false keeps the exact Conn reachable through the private error.
func sqliteCaptureClose(c *sqliteio.Conn) error {
	if c == nil {
		return nil
	}
	terminal, first := c.CloseChecked(context.Background())
	if terminal {
		return first
	}
	terminal, second := c.CloseChecked(context.Background())
	cause := errors.Join(first, second)
	if !terminal {
		return &sqliteCaptureCleanupError{public: failure("local_write_unknown"), evidence: cause, owner: c}
	}
	return cause
}
func sqliteCaptureCleanup(c *sqliteio.Conn, tx *sqliteio.Tx) error {
	var rollback error
	if tx != nil {
		rollback = tx.Rollback()
	}
	return errors.Join(rollback, sqliteCaptureClose(c))
}

// Follow only the primary branch of a checked join. A native Error is a leaf
// for classification: its Cleanup and wrapped Cause are evidence, not a new
// primary category. In particular, only bare sanitized ENOSPC qualifies.
func sqliteCapturePrimaryNative(err error) *sqliteio.Error {
	if native, ok := err.(*sqliteio.Error); ok {
		return native
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if cause != nil {
				return sqliteCapturePrimaryNative(cause)
			}
		}
		return nil
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return sqliteCapturePrimaryNative(wrapped.Unwrap())
	}
	return nil
}
func sqliteCaptureError(err error) error {
	if err == nil {
		return nil
	}
	var retained *sqliteCaptureCleanupError
	if errors.As(err, &retained) {
		return errors.Join(failure("local_write_unknown"), err)
	}
	var domain *Error
	if errors.As(err, &domain) {
		return err
	}
	// Preserve the whole checked cause tree. Classification never drops a
	// sibling error, nor promotes cleanup ENOSPC into a primary capacity cause.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sqliteio.ErrBusy) {
		return errors.Join(failure("state_busy"), err)
	}
	if errors.Is(err, sqliteio.ErrUnsafe) || errors.Is(err, sqliteio.ErrClosed) {
		return errors.Join(failure("state_corrupt"), err)
	}
	if native := sqliteCapturePrimaryNative(err); native != nil {
		code := "state_corrupt"
		if native.Category == sqliteio.Busy || native.Category == sqliteio.Canceled {
			code = "state_busy"
		}
		if native.Category == sqliteio.Unsafe && native.Phase == sqliteio.Admission && native.Cause == os.ErrExist {
			code = "state_path_in_use"
		}
		if native.Category == sqliteio.Full || native.Category == sqliteio.IO && native.Code == 0 && native.Cause == syscall.ENOSPC {
			e := failure("validation")
			e.Details = map[string]any{"reason": "database_capacity"}
			return errors.Join(e, err)
		}
		return errors.Join(failure(code), err)
	}
	return errors.Join(failure("state_corrupt"), err)
}
func sqliteCaptureUnknown(e Event, cause error) error {
	v := failure("local_write_unknown")
	v.Details = map[string]any{"event_id": e.EventID, "actor": ActorRef{Key: e.Actor, Generation: e.Generation}, "sequence": e.Sequence}
	return errors.Join(v, sqliteCaptureError(cause))
}

// InspectForLink is the shared noncreating native inspection prerequisite. An
// inspection error can retain a cleanup-only connection; it is never usable.
func sqliteOpenCapture(ctx context.Context, a sqliteCaptureAdmission, mode sqliteio.Mode) (c *sqliteio.Conn, tx *sqliteio.Tx, meta sqliteStoreMeta, found bool, err error) {
	if mode != sqliteio.Read && mode != sqliteio.Write {
		return nil, nil, meta, false, failure("validation")
	}
	if err = sqliteCaptureDeadline(ctx, a); err != nil {
		return nil, nil, meta, false, err
	}
	c, kind, err := sqliteio.InspectForLink(ctx, a.Directory, a.StateBasename, a.DatabaseBasename, a.AcquireDeadline)
	fail := func(cause error) (*sqliteio.Conn, *sqliteio.Tx, sqliteStoreMeta, bool, error) {
		return nil, nil, sqliteStoreMeta{}, false, sqliteCaptureError(errors.Join(cause, sqliteCaptureCleanup(c, tx)))
	}
	if err != nil {
		return fail(err)
	}
	if kind == sqliteio.LinkAbsent {
		if c != nil {
			return fail(failure("state_corrupt"))
		}
		return nil, nil, meta, false, nil
	}
	if kind != sqliteio.LinkWAL || c == nil {
		return fail(failure("state_corrupt"))
	}
	if mode == sqliteio.Write {
		if err = sqliteCaptureClose(c); err != nil {
			return nil, nil, sqliteStoreMeta{}, false, sqliteCaptureError(err)
		}
		c = nil
		if err = sqliteCaptureDeadline(ctx, a); err != nil {
			return fail(err)
		}
		c, err = sqliteio.Open(ctx, a.Directory, a.DatabaseBasename, sqliteio.Options{AcquireDeadline: a.AcquireDeadline})
		if err != nil {
			return fail(err)
		}
	}
	tx, err = c.Begin(ctx, mode)
	if err != nil {
		return fail(err)
	}
	if mode == sqliteio.Write {
		if err = tx.CheckAuthorityAbsent(a.StateBasename); err != nil {
			return fail(err)
		}
	}
	meta, err = sqliteReadCaptureSchema(tx, a.StateBasename, a.DatabaseBasename)
	if err != nil {
		return fail(err)
	}
	return c, tx, meta, true, nil
}

func (s *Service) ingestSQLite(ctx context.Context, e Event) (EventResult, error) {
	if err := validateEvent(e); err != nil {
		return EventResult{}, err
	}
	if e.Parent != nil {
		p := *e.Parent
		e.Parent = &p
	}
	directory, authority, database, err := sqliteLocation(s.store.path)
	if err != nil {
		return EventResult{}, err
	}
	timeout := s.store.timeout
	if timeout == 0 {
		timeout = 250 * time.Millisecond
	}
	if timeout < 0 || timeout > time.Second {
		return EventResult{}, failure("validation")
	}
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	admission := sqliteCaptureAdmission{Directory: directory, StateBasename: authority, DatabaseBasename: database, AcquireDeadline: deadline}
	prepared, clock, found, err := s.sqlitePrepareCapture(ctx, e, admission)
	historical := prepared.Receipt != nil && prepared.Receipt.Value.Fingerprint == sqliteCaptureFingerprint(e)
	if err != nil {
		if historical {
			return EventResult{}, sqliteCaptureUnknown(e, err)
		}
		return EventResult{}, sqliteCaptureError(err)
	}
	if !found {
		return baseResult(e, "0", "untracked"), nil
	}
	c, tx, meta, found, err := sqliteOpenCapture(ctx, admission, sqliteio.Write)
	if err != nil || !found {
		if err == nil {
			err = failure("state_busy")
		}
		if historical {
			return EventResult{}, sqliteCaptureUnknown(e, err)
		}
		return EventResult{}, sqliteCaptureError(err)
	}
	fail := func(cause error, uncertain bool) (EventResult, error) {
		cleanup := sqliteCaptureCleanup(c, tx)
		if uncertain || historical || cleanup != nil {
			return EventResult{}, sqliteCaptureUnknown(e, errors.Join(cause, cleanup))
		}
		return EventResult{}, sqliteCaptureError(cause)
	}
	// Re-observe writer identity/replay before pressure: a concurrently committed
	// exact receipt is already historical even when its nonce fence cannot fit.
	transition, terminal, err := sqliteCapturePriority(tx, meta, e)
	if err != nil {
		return fail(err, false)
	}
	if terminal && transition.OperationError != nil {
		return fail(transition.OperationError, false)
	}
	if terminal && transition.Result.Disposition == "duplicate" {
		historical = true
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return fail(err, false)
	}
	if !terminal {
		transition, err = sqliteReduceEvent(tx, meta, e, prepared, &clock, false)
		if err != nil {
			return fail(err, false)
		}
	}
	if !transition.Changed && transition.OperationError != nil {
		return fail(transition.OperationError, false)
	}
	// Staged proposals discarded by a false Changed predicate must be rolled back;
	// only no-write successful early outcomes may receive a nonce fence.
	if !transition.Changed && transition.Delta != 0 {
		return fail(failure("state_corrupt"), false)
	}
	after := meta
	if transition.Changed {
		if meta.Revision == "18446744073709551615" {
			return fail(failure("validation"), false)
		}
		after.Revision = bump(meta.Revision)
		if err = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, after.Revision, transition.Dependencies); err != nil {
			return fail(err, false)
		}
		if err = sqliteValidateSelectedFinalization(tx, meta.ComputerID, after.Revision, transition.Finalization); err != nil {
			return fail(err, false)
		}
	}
	if err = sqliteFinalizationAdd(&after.LogicalBytes, transition.Delta, nil); err != nil {
		return fail(err, false)
	}
	after.DurabilityNonce, err = sqliteNextNonce(meta.DurabilityNonce[:])
	if err != nil {
		return fail(err, false)
	}
	if err = sqliteUpdateMeta(tx, meta, after); err != nil {
		return fail(err, false)
	}
	outcome, err := tx.Commit()
	if err != nil || outcome != sqliteio.Committed {
		if err == nil {
			err = failure("state_corrupt")
		}
		return fail(err, outcome == sqliteio.Unknown || outcome == sqliteio.Committed)
	}
	if err = c.CloseDurably(ctx); err != nil {
		return EventResult{}, sqliteCaptureUnknown(e, errors.Join(err, sqliteCaptureClose(c)))
	}
	if transition.OperationError != nil {
		return transition.Result, transition.OperationError
	}
	return transition.Result, nil
}
func sqliteCaptureCapacity(tx *sqliteio.Tx, meta sqliteStoreMeta) error {
	if err := sqliteMetaCapacity(meta); err != nil {
		return err
	}
	pages, err := tx.PageInfo()
	if err != nil {
		return err
	}
	files, err := tx.Footprint()
	if err != nil {
		return err
	}
	if pages.PageSize != sqliteio.PageSize || pages.MaxPages > sqliteio.MaxPages {
		return failure("state_corrupt")
	}
	reason := ""
	if pages.PageCount > sqliteio.MaxPages {
		reason = "database_capacity"
	}
	if files.WAL >= 64<<20 || files.Total >= 320<<20 {
		reason = "maintenance_required"
	}
	if reason != "" {
		e := failure("validation")
		e.Details = map[string]any{"reason": reason}
		return e
	}
	return nil
}

func sqliteCaptureInitial(tx *sqliteio.Tx, meta sqliteStoreMeta, e Event) (p sqliteCapturePreparation, err error) {
	p.ComputerID = meta.ComputerID
	if e.Actor.ComputerID != meta.ComputerID {
		return p, nil
	}
	receipt, ok, err := sqliteReadEventReceipt(tx, eventKey(e), meta.Revision)
	if err != nil {
		return p, err
	}
	if ok {
		p.Receipt = &receipt
		return p, nil
	}
	key, ok, err := sqliteReadEventID(tx, e.EventID)
	if err != nil {
		return p, err
	}
	if ok {
		p.EventIDKey = &key
		if key != eventKey(e) {
			return p, nil
		}
	}
	p.Target, err = sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, e.Actor)
	if err != nil {
		return p, err
	}
	p.TargetChecked = true
	_, _, p.Branch, _ = sqliteCaptureOrder(meta, e, p.Target)
	p.Binding.Needed = p.Branch == sqliteCaptureNewSample
	return p, nil
}
func (s *Service) sqlitePrepareCapture(ctx context.Context, e Event, a sqliteCaptureAdmission) (p sqliteCapturePreparation, clock sqliteCaptureClock, found bool, err error) {
	clock.Mode = sqliteCapturePrepared
	if s.nativeCaptureClock {
		clock.Mode = sqliteCaptureNative
	}
	if err = validateEvent(e); err != nil {
		return p, clock, false, err
	}
	if e.Parent != nil {
		parent := *e.Parent
		e.Parent = &parent
	}
	c, tx, meta, found, err := sqliteOpenCapture(ctx, a, sqliteio.Read)
	if err != nil || !found {
		return p, clock, found, err
	}
	p, err = sqliteCaptureInitial(tx, meta, e)
	if err == nil && p.Binding.Needed && s.resolve == nil && e.BindingID != "" {
		p.Binding.ExplicitChecked = true
		row, ok, x := sqliteReadBinding(tx, meta.ComputerID, e.BindingID)
		err = x
		if ok {
			p.Binding.Explicit = &row
		}
	}
	if err == nil && !p.Binding.Needed && p.Branch != sqliteCaptureNoSample && clock.Mode == sqliteCapturePrepared {
		p.ClockActors, err = sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
	}
	err = errors.Join(err, sqliteCaptureCleanup(c, tx))
	if err != nil {
		return p, clock, true, err
	}
	if p.Branch == sqliteCaptureNoSample {
		return p, clock, true, nil
	}
	if p.Binding.Needed {
		var location *Location
		parentFallback := false
		if s.resolve != nil {
			if err = sqliteCaptureDeadline(ctx, a); err != nil {
				return p, clock, true, err
			}
			b, ok, resolveErr := s.resolve(ctx, e)
			if resolveErr != nil {
				p.Binding.ErrorCode = "binding_unavailable"
			} else if ok {
				p.Binding.Snapshot = &b
			} else if e.BindingID == "" {
				parentFallback = true
			}
		} else {
			if e.BindingID != "" {
				row := p.Binding.Explicit
				if row == nil || row.Record == nil || row.Record.Deleted || row.Snapshot.Revision != e.BindingRevision {
					p.Binding.ErrorCode = "binding_unavailable"
				} else {
					if err = sqliteCaptureDeadline(ctx, a); err != nil {
						return p, clock, true, err
					}
					if x := bindingAvailable(ctx, *row.Record); x != nil {
						p.Binding.ErrorCode = sqliteCaptureBindingError(x)
					} else {
						b := row.Snapshot
						p.Binding.Snapshot = &b
					}
				}
			}
			if p.Binding.ErrorCode == "" && e.CWD != "" {
				if err = sqliteCaptureDeadline(ctx, a); err != nil {
					return p, clock, true, err
				}
				loc, x := DiscoverLocation(ctx, e.CWD)
				if x != nil {
					p.Binding.ErrorCode = sqliteCaptureBindingError(x)
				} else {
					location = &loc
				}
			}
			if e.BindingID == "" {
				parentFallback = true
			}
		}
		if p.Binding.ErrorCode == "" {
			c, tx, meta, ok, x := sqliteOpenCapture(ctx, a, sqliteio.Read)
			if x != nil {
				return p, clock, true, x
			}
			if !ok {
				return p, clock, true, failure("state_busy")
			}
			err = sqliteCaptureRecheckInitial(tx, meta, e, p)
			if err != nil {
				var d *Error
				if errors.As(err, &d) && d.Code == "state_busy" {
					p.RefusalCode = "state_busy"
					err = nil
				}
			}
			if err == nil && p.RefusalCode == "" && location != nil {
				var row *sqliteBindingRow
				row, p.Binding.Locations, err = sqliteCaptureLocation(tx, meta.ComputerID, *location)
				if err == nil {
					if e.BindingID != "" {
						if row == nil || row.Snapshot.ID != e.BindingID {
							p.Binding.ErrorCode = "binding_unavailable"
						}
					} else if row != nil {
						b := row.Snapshot
						p.Binding.Snapshot = &b
						parentFallback = false
					}
				}
			}
			if err == nil && p.RefusalCode == "" && p.Binding.ErrorCode == "" && p.Binding.Snapshot == nil && parentFallback && e.Parent != nil {
				p.Binding.ParentChecked = true
				p.Binding.Parent, err = sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, e.Parent.Key)
				if parent := p.Binding.Parent; err == nil && parent != nil && parent.Ref.Generation == e.Parent.Generation && !sqliteCaptureTerminal(*parent) {
					p.Binding.Snapshot = &BindingSnapshot{ID: parent.BindingID, Revision: parent.BindingRevision, Attribution: parent.Attribution}
				}
			}
			if b := p.Binding.Snapshot; err == nil && p.RefusalCode == "" && p.Binding.ErrorCode == "" && b != nil {
				_, validRevision := counter(b.Revision)
				if !validUUID(b.ID) || !validAttribution(b.Attribution) || !validRevision || e.BindingID != "" && (b.ID != e.BindingID || b.Revision != e.BindingRevision) {
					p.Binding.ErrorCode = "binding_unavailable"
				} else {
					p.Binding.TimerChecked = true
					p.Binding.Timer, err = sqliteCaptureTimerBindings(tx, meta.ComputerID, b.Attribution)
					if err == nil {
						for _, row := range p.Binding.Timer {
							if row.Snapshot.Attribution != b.Attribution {
								attribution := row.Snapshot.Attribution
								p.Binding.Conflict = &attribution
								p.Binding.ErrorCode = "attribution_conflict"
								break
							}
						}
					}
				}
			}
			if err == nil && p.RefusalCode == "" && p.Binding.ErrorCode == "" && p.Binding.Snapshot != nil && clock.Mode == sqliteCapturePrepared {
				p.ClockActors, err = sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
			}
			err = errors.Join(err, sqliteCaptureCleanup(c, tx))
			if err != nil {
				return p, clock, true, err
			}
		}
		if p.RefusalCode != "" || p.Binding.ErrorCode != "" || p.Binding.Snapshot == nil {
			p.Branch = sqliteCaptureNoSample
			return p, clock, true, nil
		}
	}
	clock.Site = sqliteCaptureSite(p.Branch)
	if clock.Mode == sqliteCapturePrepared {
		if err = sqliteCaptureDeadline(ctx, a); err != nil {
			return p, clock, true, err
		}
		sample, sampleErr := s.sample()
		clock.Prepared = &sqliteCaptureSample{Value: sqliteCaptureCopySample(sample), Unavailable: sampleErr != nil}
	}
	return p, clock, true, nil
}
func sqliteCaptureBindingError(err error) string {
	var e *Error
	if errors.As(err, &e) && e.Code == "validation" {
		return "validation"
	}
	return "binding_unavailable"
}
func sqliteCaptureLocation(tx *sqliteio.Tx, computer string, loc Location) (*sqliteBindingRow, []sqliteCaptureBindingLookup, error) {
	lookups := []sqliteCaptureBindingLookup{}
	locator := loc.Locator
	if loc.Kind == "directory" {
		locator = loc.Path
	} else if loc.Kind != "repository" {
		return nil, nil, failure("validation")
	}
	for {
		row, ok, err := sqliteReadBindingLocation(tx, computer, loc.Kind, locator)
		if err != nil {
			return nil, nil, err
		}
		lookup := sqliteCaptureBindingLookup{Kind: loc.Kind, Locator: locator}
		if ok {
			lookup.Row = &row
		}
		lookups = append(lookups, lookup)
		if ok {
			return &row, lookups, nil
		}
		if loc.Kind == "repository" {
			return nil, lookups, nil
		}
		parent := filepath.Dir(locator)
		if parent == locator {
			return nil, lookups, nil
		}
		locator = parent
	}
}
func sqliteCaptureRecheckInitial(tx *sqliteio.Tx, meta sqliteStoreMeta, e Event, p sqliteCapturePreparation) error {
	if meta.ComputerID != p.ComputerID {
		return failure("state_busy")
	}
	current, err := sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, e.Actor)
	if err != nil {
		return err
	}
	same, err := sqliteCaptureSameActor(current, p.Target)
	if err != nil {
		return err
	}
	if !same {
		return failure("state_busy")
	}
	if p.Binding.ExplicitChecked {
		var current *sqliteBindingRow
		r, ok, err := sqliteReadBinding(tx, meta.ComputerID, e.BindingID)
		if err != nil {
			return err
		}
		if ok {
			current = &r
		}
		same, err := sqliteCaptureSameBinding(current, p.Binding.Explicit)
		if err != nil {
			return err
		}
		if !same {
			return failure("state_busy")
		}
	}
	return nil
}
func sqliteCaptureCheckBinding(tx *sqliteio.Tx, meta sqliteStoreMeta, e Event, b sqliteCaptureBinding) error {
	if !b.Needed {
		if b.Snapshot != nil || b.ErrorCode != "" || b.Conflict != nil || b.ExplicitChecked || b.Explicit != nil || len(b.Locations) != 0 || b.ParentChecked || b.Parent != nil || b.TimerChecked || len(b.Timer) != 0 {
			return failure("validation")
		}
		return nil
	}
	if b.ErrorCode != "" && b.ErrorCode != "validation" && b.ErrorCode != "binding_unavailable" && b.ErrorCode != "attribution_conflict" {
		return failure("validation")
	}
	if b.ExplicitChecked {
		if e.BindingID == "" {
			return failure("validation")
		}
		var current *sqliteBindingRow
		r, ok, err := sqliteReadBinding(tx, meta.ComputerID, e.BindingID)
		if err != nil {
			return err
		}
		if ok {
			current = &r
		}
		same, err := sqliteCaptureSameBinding(current, b.Explicit)
		if err != nil {
			return err
		}
		if !same {
			return failure("state_busy")
		}
	}
	for _, lookup := range b.Locations {
		var current *sqliteBindingRow
		r, ok, err := sqliteReadBindingLocation(tx, meta.ComputerID, lookup.Kind, lookup.Locator)
		if err != nil {
			return err
		}
		if ok {
			current = &r
		}
		same, err := sqliteCaptureSameBinding(current, lookup.Row)
		if err != nil {
			return err
		}
		if !same {
			return failure("state_busy")
		}
	}
	if b.ParentChecked {
		if e.Parent == nil {
			return failure("validation")
		}
		current, err := sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, e.Parent.Key)
		if err != nil {
			return err
		}
		same, err := sqliteCaptureSameActor(current, b.Parent)
		if err != nil {
			return err
		}
		if !same {
			return failure("state_busy")
		}
	}
	if b.TimerChecked {
		if b.Snapshot == nil {
			return failure("validation")
		}
		rows, err := sqliteCaptureTimerBindings(tx, meta.ComputerID, b.Snapshot.Attribution)
		if err != nil {
			return err
		}
		if len(rows) != len(b.Timer) {
			return failure("state_busy")
		}
		for i := range rows {
			same, err := sqliteCaptureSameBinding(&rows[i], &b.Timer[i])
			if err != nil {
				return err
			}
			if !same {
				return failure("state_busy")
			}
		}
	}
	return nil
}

// Equality compares the fixed stored projection, including the original wall
// JSON offset, rather than time.Time's location identity or instant alone.
func sqliteCaptureSameActor(a, b *sqliteActorLocalRow) (bool, error) {
	if a == nil || b == nil {
		return a == nil && b == nil, nil
	}
	x, err := sqliteEncodeActorLocal(*a)
	if err != nil {
		return false, err
	}
	y, err := sqliteEncodeActorLocal(*b)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(x.values, y.values), nil
}
func sqliteCaptureSameBinding(a, b *sqliteBindingRow) (bool, error) {
	if a == nil || b == nil {
		return a == nil && b == nil, nil
	}
	x, err := sqliteEncodeBinding(*a)
	if err != nil {
		return false, err
	}
	y, err := sqliteEncodeBinding(*b)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(x.values, y.values), nil
}
func sqliteCaptureSameClock(a, b []sqliteCaptureClockActor) (bool, error) {
	if len(a) != len(b) {
		return false, nil
	}
	for i := range a {
		x, y := a[i], b[i]
		same, err := sqliteCaptureSameActor(&x.Actor, &y.Actor)
		if err != nil || !same {
			return same, err
		}
		if (x.Segment == nil) != (y.Segment == nil) || (x.Epoch == nil) != (y.Epoch == nil) || (x.LatestWait == nil) != (y.LatestWait == nil) || len(x.WaitTurns) != len(y.WaitTurns) {
			return false, nil
		}
		if x.Segment != nil {
			left, e := sqliteEncodeSegmentLocal(*x.Segment)
			if e != nil {
				return false, e
			}
			right, e := sqliteEncodeSegmentLocal(*y.Segment)
			if e != nil {
				return false, e
			}
			if !reflect.DeepEqual(left.values, right.values) {
				return false, nil
			}
		}
		if x.Epoch != nil {
			left, e := sqliteEncodeEpochRow(*x.Epoch)
			if e != nil {
				return false, e
			}
			right, e := sqliteEncodeEpochRow(*y.Epoch)
			if e != nil {
				return false, e
			}
			if !reflect.DeepEqual(left.values, right.values) {
				return false, nil
			}
		}
		if x.LatestWait != nil {
			left, e := sqliteEncodeHostReceipt(*x.LatestWait)
			if e != nil {
				return false, e
			}
			right, e := sqliteEncodeHostReceipt(*y.LatestWait)
			if e != nil {
				return false, e
			}
			if !reflect.DeepEqual(left.values, right.values) {
				return false, nil
			}
		}
		for j := range x.WaitTurns {
			left, e := sqliteEncodeHostTurn(x.Actor.Ref.Key.ComputerID, x.WaitTurns[j].Row)
			if e != nil {
				return false, e
			}
			right, e := sqliteEncodeHostTurn(y.Actor.Ref.Key.ComputerID, y.WaitTurns[j].Row)
			if e != nil {
				return false, e
			}
			if !reflect.DeepEqual(left.values, right.values) || !reflect.DeepEqual(x.WaitTurns[j].Pending, y.WaitTurns[j].Pending) {
				return false, nil
			}
		}
	}
	return true, nil
}
