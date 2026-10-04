//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

// LinkInspection is an observation, never permission to initialize later.
// Its value is unusable whenever InspectForLink returns an error.
type LinkInspection uint8

const (
	LinkAbsent LinkInspection = iota
	LinkPristine
	LinkWAL
)

// InspectForLink does not create an authority guard, main file or directory.
// Absent/pristine success owns nothing; WAL success returns one unused read-only
// Conn. A nonnil Conn on error is the exact cleanup-only owner: the caller must
// retain it through checked ordinary Close and may not Begin or acknowledge it.
func InspectForLink(ctx context.Context, directory, stateBasename, databaseBasename string, deadline time.Time) (*Conn, LinkInspection, error) {
	return inspectForLink(ctx, directory, stateBasename, databaseBasename, deadline, false)
}

// InspectForCaptureWrite keeps the noncreating read-only pager prerequisite for
// a subsequent, separately checked writable Open. WAL success returns a
// cleanup-only Conn: Begin is disabled, and ordinary checked close is required
// before Open. It does not validate the application catalog or grant a write.
// Absent/pristine and error ownership match InspectForLink. The same absolute
// deadline must cover this probe, checked close, writable Open and Write Begin.
func InspectForCaptureWrite(ctx context.Context, directory, stateBasename, databaseBasename string, deadline time.Time) (*Conn, LinkInspection, error) {
	return inspectForLink(ctx, directory, stateBasename, databaseBasename, deadline, true)
}

func inspectForLink(ctx context.Context, directory, stateBasename, databaseBasename string, deadline time.Time, writePrerequisite bool) (*Conn, LinkInspection, error) {
	if err := admissionError(ctx, deadline); err != nil {
		return nil, 0, safeError(Admission, err)
	}
	if !validInitialNames(stateBasename, databaseBasename) {
		return nil, 0, &Error{Phase: Admission, Category: Invalid}
	}
	id, absent, err := inspectLinkParent(ctx, directory, deadline)
	if err != nil {
		return nil, 0, err
	}
	if absent {
		return nil, LinkAbsent, nil
	}
	r, err := acquireRoot(ctx, directory, databaseBasename, false, deadline)
	if err != nil {
		return nil, 0, safeError(Admission, err)
	}
	c := newConnection(r, Options{ReadOnly: true, AcquireDeadline: deadline})
	fail := func(cause error) (*Conn, LinkInspection, error) {
		owner, failure := failOpen(c, combineClose(cause, initialProbeError(r)))
		return owner, 0, failure
	}
	admit, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if r.id != id {
		return fail(safeError(Admission, ErrUnsafe))
	}
	// Every authority observation is through the leased directory. Rechecking
	// context and root after the stat prevents an intervening failure from
	// being interpreted as absence; no P descriptor is opened.
	check := func() error {
		if err := admissionError(admit, deadline); err != nil {
			return safeError(Admission, err)
		}
		if err := validateRoot(r); err != nil {
			return safeError(VerifyPhase, err)
		}
		err := requireAuthorityAbsent(r, stateBasename)
		if verify := validateRoot(r); verify != nil {
			return safeError(VerifyPhase, verify)
		}
		if cause := admissionError(admit, deadline); cause != nil {
			return safeError(Admission, cause)
		}
		return err
	}
	if err = check(); err != nil {
		return fail(err)
	}
	var st unix.Stat_t
	err = unix.Fstatat(r.fd, databaseBasename, &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		if err = noSidecars(r); err != nil {
			return fail(err)
		}
		if err = requireAbsent(r, databaseBasename); err != nil {
			return fail(err)
		}
		if err = check(); err != nil {
			return fail(err)
		}
		// No native handle or watcher exists on this branch. Release only the
		// root lease, without entering a native close or initialization path.
		c.finishRelease(nil)
		if c.closeErr != nil {
			return nil, 0, c.closeErr
		}
		if err = admissionError(admit, deadline); err != nil {
			return nil, 0, safeError(Admission, err)
		}
		return nil, LinkAbsent, nil
	}
	if err != nil {
		return fail(safeError(Admission, err))
	}
	if !privateFile(&st) {
		return fail(safeError(Admission, ErrUnsafe))
	}
	if err = initialize(); err != nil {
		return fail(safeError(OpenPhase, err))
	}
	beginInitialProbe(r)
	if err = c.openNative(admit, Options{ReadOnly: true, AcquireDeadline: deadline}); err != nil {
		return fail(err)
	}
	stop := c.interruptWith(admit)
	pristine, proofErr := c.initialMainProfile(admit)
	if proofErr == nil && !pristine {
		proofErr = finishInitialProbe(r)
	}
	var image []byte
	var mode string
	if proofErr == nil && writePrerequisite && !pristine {
		// A real read transaction reaches SQLite's read-only hot-journal
		// refusal and WAL recovery without parsing the application schema.
		// The later writable Open still performs every setup/policy check;
		// the activity writer still checks exact catalog/meta after BEGIN.
		_, proofErr = c.probeInteger(admit, "PRAGMA schema_version")
	} else if proofErr == nil {
		mode, proofErr = c.probeText(admit, "PRAGMA journal_mode")
		if proofErr == nil {
			if pristine && mode == "delete" {
				image, proofErr = c.provePristine(admit)
			} else if !pristine && mode == "wal" {
				// readOnly skips page/max-page/journal-limit setters. The remaining
				// four setters are connection-local; journal_mode is only queried.
				proofErr = c.setup(admit, false)
			} else {
				proofErr = safeError(OpenPhase, ErrUnsafe)
			}
		}
	}
	stop()
	if proofErr != nil {
		return fail(proofErr)
	}
	if err = check(); err != nil {
		return fail(err)
	}
	if !pristine {
		if writePrerequisite {
			// This handle has not run full connection setup. Reuse the existing
			// terminal-use guard so it can only be closed, never admitted.
			c.used = true
		}
		return c, LinkWAL, nil
	}
	// Probe statements have finalized and the watcher has joined. Retain the
	// lease until the closed native image, identity and sidecars are rechecked.
	if err = c.closeNative(); err != nil {
		return fail(err)
	}
	if err = admissionError(admit, deadline); err != nil {
		return fail(safeError(Admission, err))
	}
	if err = closedPristineImage(r, image); err != nil {
		return fail(err)
	}
	if err = initialProbeError(r); err != nil {
		return fail(err)
	}
	if err = check(); err != nil {
		return fail(err)
	}
	// Ordinary Close also covers root-only ownership after closeNative. An
	// inconclusive close leaves this exact owner with the caller, never success.
	if err = c.Close(context.Background()); err != nil {
		if c.closed {
			return nil, 0, err
		}
		return c, 0, err
	}
	if err = admissionError(admit, deadline); err != nil {
		return nil, 0, safeError(Admission, err)
	}
	return nil, LinkPristine, nil
}

// CheckAuthorityAbsent must be the caller's first observation after actual
// Write Begin. It consumes no SQL statement or new admission budget and does
// not reserve absence against a later external creator.
func (t *Tx) CheckAuthorityAbsent(stateBasename string) error {
	if err := t.check(Admission); err != nil {
		return err
	}
	if t.mode != Write || len(t.statements) != 0 {
		return misuse(Admission)
	}
	if !validInitialNames(stateBasename, t.conn.root.databaseName) {
		return &Error{Phase: Admission, Category: Invalid}
	}
	err := requireAuthorityAbsent(t.conn.root, stateBasename)
	if checked := t.check(Admission); checked != nil {
		return checked
	}
	return err
}
