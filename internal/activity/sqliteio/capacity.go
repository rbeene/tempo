//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"math"
	"strconv"

	"golang.org/x/sys/unix"
	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

const (
	PageSize          int64 = 4096
	MaxPages          int64 = 65536
	JournalLimitBytes int64 = 4 << 20
)

type PageInfo struct {
	PageSize, PageCount, MaxPages, JournalLimitBytes int64
}

// Footprint reports sequential file-length observations, not an atomic quota.
type Footprint struct {
	Main, WAL, SHM, Journal, Total int64
}

type pagePragma uint8

const (
	readPageSize pagePragma = iota
	readPageCount
	readMaxPages
	readJournalLimit
	setMaxPages
	setJournalLimit
)

// pageValue permits only these fixed policy controls, with one INTEGER and DONE.
// The caller owns the gate and cancellation watcher; no new budget is started.
func (c *Conn) pageValue(ctx context.Context, query pagePragma, phase Phase) (value int64, err error) {
	var sql string
	switch query {
	case readPageSize:
		sql = "PRAGMA page_size"
	case readPageCount:
		sql = "PRAGMA page_count"
	case readMaxPages:
		sql = "PRAGMA max_page_count"
	case readJournalLimit:
		sql = "PRAGMA journal_size_limit"
	case setMaxPages:
		if c.maxPages <= 0 || c.maxPages > MaxPages {
			return 0, misuse(phase)
		}
		sql = "PRAGMA max_page_count=" + strconv.FormatInt(c.maxPages, 10)
	case setJournalLimit:
		sql = "PRAGMA journal_size_limit=4194304"
	default:
		return 0, misuse(phase)
	}
	if err = ctx.Err(); err != nil {
		return 0, safeError(phase, err)
	}
	if phase == OpenPhase {
		if err = admissionError(ctx, c.acquireDeadline); err != nil {
			return 0, safeError(phase, err)
		}
		if err = c.busyRemaining(ctx); err != nil {
			return 0, err
		}
	}
	libc.AssignPtrUint32(c.authMode, authPragma)
	defer func() { libc.AssignPtrUint32(c.authMode, authApplication) }()
	stmt, err := c.prepareRaw(sql, phase)
	if err != nil {
		return 0, contextualError(phase, err, ctx)
	}
	defer func() {
		if rc := lib.Xsqlite3_finalize(c.tls, stmt); rc != lib.SQLITE_OK {
			cleanup := engineError(FinalizePhase, rc, ctx)
			if err == nil {
				err = cleanup
			} else {
				e := safeError(phase, err)
				e.Cleanup = joinCleanup(e.Cleanup, cleanup)
				err = e
			}
		}
	}()
	if lib.Xsqlite3_column_count(c.tls, stmt) != 1 {
		return 0, misuse(phase)
	}
	for step := 0; step < 2; step++ {
		if err = ctx.Err(); err != nil {
			return 0, safeError(phase, err)
		}
		if phase == OpenPhase {
			if err = c.busyRemaining(ctx); err != nil {
				return 0, err
			}
		}
		rc := lib.Xsqlite3_step(c.tls, stmt)
		if step == 0 && rc == lib.SQLITE_ROW {
			if lib.Xsqlite3_column_type(c.tls, stmt, 0) != lib.SQLITE_INTEGER {
				return 0, misuse(phase)
			}
			value = lib.Xsqlite3_column_int64(c.tls, stmt, 0)
		} else if step == 1 && rc == lib.SQLITE_DONE {
			return value, nil
		} else if rc == lib.SQLITE_ROW || rc == lib.SQLITE_DONE {
			return 0, misuse(phase)
		} else {
			return 0, engineError(phase, rc, ctx)
		}
	}
	return 0, misuse(phase)
}

func (c *Conn) pageInfo(ctx context.Context, phase Phase) (info PageInfo, err error) {
	for _, item := range []struct {
		query pagePragma
		value *int64
	}{
		{readPageSize, &info.PageSize}, {readPageCount, &info.PageCount},
		{readMaxPages, &info.MaxPages}, {readJournalLimit, &info.JournalLimitBytes},
	} {
		*item.value, err = c.pageValue(ctx, item.query, phase)
		if err != nil {
			return PageInfo{}, err
		}
	}
	if info.PageSize != PageSize || info.PageCount < 0 || info.MaxPages < info.PageCount || info.MaxPages <= 0 {
		return PageInfo{}, safeError(phase, ErrUnsafe)
	}
	if !c.readOnly {
		wantMax := c.maxPages
		if info.PageCount > wantMax {
			wantMax = info.PageCount
		}
		if info.MaxPages != wantMax || info.JournalLimitBytes != JournalLimitBytes {
			return PageInfo{}, safeError(phase, ErrUnsafe)
		}
	}
	return info, nil
}

func (c *Conn) setupPagePolicy(ctx context.Context) error {
	pages, err := c.pageValue(ctx, readPageCount, OpenPhase)
	if err != nil {
		return err
	}
	if pages == 0 && !c.readOnly {
		files, err := c.footprint()
		if err != nil {
			return err
		}
		// Create alone cannot establish freshness. Both native page state and
		// every pinned role must be empty before changing the page size.
		if files.Total == 0 {
			if _, err = c.control(ctx, "PRAGMA page_size=4096", authPragma, OpenPhase); err != nil {
				return err
			}
		}
	}
	size, err := c.pageValue(ctx, readPageSize, OpenPhase)
	if err != nil {
		return err
	}
	if size != PageSize {
		return safeError(OpenPhase, ErrUnsafe)
	}
	if !c.readOnly {
		for _, query := range []pagePragma{setMaxPages, setJournalLimit} {
			if _, err = c.pageValue(ctx, query, OpenPhase); err != nil {
				return err
			}
		}
	}
	info, err := c.pageInfo(ctx, OpenPhase)
	if err != nil {
		return err
	}
	c.oversize = info.PageCount > c.maxPages
	return nil
}

func (t *Tx) PageInfo() (info PageInfo, err error) {
	defer func() { t.readFailure(err) }()
	if err = t.check(VerifyPhase); err != nil {
		return info, err
	}
	if len(t.statements) != 0 {
		return info, misuse(VerifyPhase)
	}
	info, err = t.conn.pageInfo(t.ctx, VerifyPhase)
	if err == nil && t.mode == Write && info.PageCount > t.conn.maxPages {
		err = &Error{Phase: VerifyPhase, Category: Full}
	}
	if err == nil {
		err = t.check(VerifyPhase)
	}
	return info, err
}

func (t *Tx) Footprint() (files Footprint, err error) {
	defer func() { t.readFailure(err) }()
	if err = t.check(VerifyPhase); err != nil {
		return files, err
	}
	if len(t.statements) != 0 {
		return files, misuse(VerifyPhase)
	}
	files, err = t.conn.footprint()
	if err == nil {
		err = t.check(VerifyPhase)
	}
	return files, err
}

func (f *Footprint) sum() error {
	f.Total = 0
	for _, size := range [...]int64{f.Main, f.WAL, f.SHM, f.Journal} {
		if size < 0 || size > math.MaxInt64-f.Total {
			return ErrUnsafe
		}
		f.Total += size
	}
	return nil
}

// footprint never opens another database or SHM descriptor. Observed roles
// join the existing identity ledger so their later disappearance is unsafe.
func (c *Conn) footprint() (files Footprint, err error) {
	r := c.root
	if err = validateRoot(r); err != nil {
		return files, safeError(VerifyPhase, err)
	}
	for _, item := range []struct {
		suffix string
		size   *int64
	}{
		{"", &files.Main}, {"-wal", &files.WAL},
		{"-shm", &files.SHM}, {"-journal", &files.Journal},
	} {
		name := r.databaseName + item.suffix
		if err = preflight(r, name, item.suffix != ""); err != nil {
			return Footprint{}, safeError(VerifyPhase, err)
		}
		emit(pathEvent(r, name, "footprint", "before", 0, -1, nil))
		var stat unix.Stat_t
		err = unix.Fstatat(r.fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW)
		registry.Lock()
		expected := r.known[name]
		bad := r.poisoned || registry.poisoned || registry.fatal
		missing := errors.Is(err, unix.ENOENT) && item.suffix != "" && expected == (identity{})
		if err == nil {
			id := fileIdentity(&stat)
			bad = bad || !privateFile(&stat) || expected != (identity{}) && id != expected || aliasLocked(id, r, name)
			if !bad {
				r.known[name] = id
			}
		}
		registry.Unlock()
		if bad || err != nil && !missing {
			return Footprint{}, safeError(VerifyPhase, ErrUnsafe)
		}
		if !missing {
			*item.size = stat.Size
		}
		emit(pathEvent(r, name, "footprint", "after", 0, -1, nil))
	}
	if err = validateRoot(r); err != nil {
		return Footprint{}, safeError(VerifyPhase, err)
	}
	if err = files.sum(); err != nil {
		return Footprint{}, safeError(VerifyPhase, err)
	}
	return files, nil
}
