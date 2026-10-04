//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"time"

	lib "modernc.org/sqlite/lib"
)

// OpenForCaptureWrite completes the same noncreating read-only prerequisite as
// InspectForCaptureWrite, then opens and fully checks a separate native writer.
// Both briefly share one exclusive root lease, keeping SQLite's live WAL index.
// The prerequisite closes conclusively before the writer can Begin. Absent and
// pristine observations own nothing. On error, a nonnil Conn owns every residual
// native handle and is cleanup-only; its inspection value must not be used.
func OpenForCaptureWrite(ctx context.Context, directory, stateBasename, databaseBasename string, deadline time.Time) (*Conn, LinkInspection, error) {
	probe, kind, err := InspectForCaptureWrite(ctx, directory, stateBasename, databaseBasename, deadline)
	if err != nil {
		if probe != nil {
			probe.used, probe.poisoned = true, true
		}
		return probe, 0, err
	}
	if kind != LinkWAL {
		return probe, kind, nil
	}
	if probe == nil {
		return nil, 0, safeError(OpenPhase, ErrUnsafe)
	}
	// Neither pointer is published during construction. Only the writer owns
	// the root; a subordinate close can free native state but not release it.
	opts := Options{AcquireDeadline: deadline}
	c := newConnection(probe.root, opts)
	c.used, c.poisoned, c.handoffProbe = true, true, probe
	probe.borrowedRoot = true
	fail := func(cause error) (*Conn, LinkInspection, error) {
		owner, failure := failOpen(c, cause)
		return owner, 0, failure
	}
	check := func() error {
		if err := admissionError(ctx, deadline); err != nil {
			return safeError(Admission, err)
		}
		if err := validateRoot(c.root); err != nil {
			return safeError(VerifyPhase, err)
		}
		err := requireAuthorityAbsent(c.root, stateBasename)
		if verify := validateRoot(c.root); verify != nil {
			return safeError(VerifyPhase, verify)
		}
		if cause := admissionError(ctx, deadline); cause != nil {
			return safeError(Admission, cause)
		}
		return err
	}
	boundary := func(phase string) error {
		if err := check(); err != nil {
			return err
		}
		if err := sqlEvent(sqlTestEvent{Phase: phase, Operation: "capture-write-handoff"}); err != nil {
			return safeError(OpenPhase, err)
		}
		// Native SHM reuse need not call our open hook. Always recheck the
		// complete observed family, including its still-live SHM identity.
		return check()
	}
	if err = boundary("handoff-before-writer"); err != nil {
		return fail(err)
	}
	if !probe.readOnly || !probe.used || probe.closed || probe.db == 0 ||
		lib.Xsqlite3_get_autocommit(probe.tls, probe.db) == 0 || lib.Xsqlite3_next_stmt(probe.tls, probe.db, 0) != 0 {
		return fail(misuse(OpenPhase))
	}
	if err = c.openNative(ctx, opts); err != nil {
		return fail(err)
	}
	if err = boundary("handoff-writer-opened"); err != nil {
		return fail(err)
	}
	setupCtx, cancel := context.WithDeadline(ctx, deadline)
	stop := c.interruptWith(setupCtx)
	err = c.setup(setupCtx, false)
	stop()
	cancel()
	if err != nil {
		return fail(err)
	}
	if err = boundary("handoff-writer-ready"); err != nil {
		return fail(err)
	}
	terminal, err := probe.CloseChecked(context.Background())
	if terminal {
		c.handoffProbe = nil
	}
	if err != nil {
		return fail(err)
	}
	if !terminal {
		return fail(safeError(ClosePhase, ErrClosed))
	}
	if err = boundary("handoff-probe-closed"); err != nil {
		return fail(err)
	}
	c.used, c.poisoned = false, false
	return c, LinkWAL, nil
}

// The caller holds c.gate. Each still-owned native slot gets one close attempt;
// failure of either cannot orphan the other or release their shared root.
func (c *Conn) closeHandoffChecked(ctx context.Context) (bool, error) {
	terminal, probeErr := c.handoffProbe.CloseChecked(ctx)
	writerTerminal := c.db == 0 && !c.nativeCounted && c.tls == nil && c.authMode == 0 && c.cancelFlag == 0
	var writerErr error
	// A prior attempt may have closed this slot while the probe stayed live.
	// An unopened writer is also terminal; a TLS-only partial open is not.
	if !writerTerminal {
		writerErr = sqlEvent(sqlTestEvent{Phase: "close-before", Operation: "close"})
		if writerErr != nil {
			writerErr = safeError(ClosePhase, writerErr)
		} else {
			writerErr = c.closeNative()
			writerTerminal = c.db == 0 && !c.nativeCounted && c.tls == nil && c.authMode == 0 && c.cancelFlag == 0
		}
	}
	c.closeErr = combineClose(c.closeErr, joinCleanup(probeErr, writerErr))
	if !terminal || !writerTerminal {
		return false, c.closeErr
	}
	c.handoffProbe = nil
	// All native owners have ended. Root release may still return a cached
	// terminal cleanup error; it does not resurrect caller ownership.
	c.finishRelease(c.closeErr)
	emit(event{Namespace: c.root.key, Role: "main", Op: "close", Phase: "closed", FD: -1})
	if err := sqlEvent(sqlTestEvent{Phase: "close-after", Operation: "close"}); err != nil {
		c.closeErr = combineClose(c.closeErr, safeError(ClosePhase, err))
	}
	return true, c.closeErr
}
