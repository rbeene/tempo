//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"strings"
	"unsafe"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

const maxSchemaBytes = 1 << 20

// InstallSchema executes trusted embedded source within the caller's write
// transaction. The caller must roll back on failure; this method never commits.
func (t *Tx) InstallSchema(ddl string) error {
	if ddl == "" || len(ddl) > maxSchemaBytes || len(ddl) >= 2147483647 || strings.ContainsRune(ddl, 0) {
		return &Error{Phase: PreparePhase, Category: Invalid}
	}
	if err := t.check(PreparePhase); err != nil {
		return err
	}
	if t.mode != Write || len(t.statements) != 0 {
		return misuse(PreparePhase)
	}
	c := t.conn
	query, err := libc.CString(ddl)
	if err != nil {
		return safeError(PreparePhase, err)
	}
	defer libc.Xfree(c.tls, query)
	output := lib.Xsqlite3_malloc64(c.tls, 16)
	if output == 0 {
		return engineError(PreparePhase, lib.SQLITE_NOMEM, nil)
	}
	defer lib.Xsqlite3_free(c.tls, output)
	end := query + uintptr(len(ddl))
	if end < query {
		return &Error{Phase: PreparePhase, Category: Invalid}
	}
	for cursor := query; cursor < end; {
		if err = t.check(PreparePhase); err != nil {
			return err
		}
		if err = sqlEvent(sqlTestEvent{Phase: "prepare-before", Operation: "prepare"}); err != nil {
			return safeError(PreparePhase, err)
		}
		if err = t.check(PreparePhase); err != nil {
			return err
		}
		*(*uintptr)(unsafe.Pointer(output)) = 0
		*(*uintptr)(unsafe.Pointer(output + 8)) = 0
		rc := lib.Xsqlite3_prepare_v2(c.tls, c.db, cursor, int32(end-cursor+1), output, output+8)
		ptr := *(*uintptr)(unsafe.Pointer(output))
		tail := *(*uintptr)(unsafe.Pointer(output + 8))
		var s *Stmt
		if ptr != 0 {
			s = &Stmt{tx: t, ptr: ptr}
			t.statements[s] = struct{}{}
		}
		if rc != lib.SQLITE_OK {
			return closeInstallStatement(s, engineError(PreparePhase, rc, t.ctx))
		}
		// A nil statement may consume a comment-only suffix. Every successful
		// prepare must advance within the original allocation, never past NUL.
		if tail <= cursor || tail > end {
			return closeInstallStatement(s, misuse(PreparePhase))
		}
		cursor = tail
		if s == nil {
			continue
		}
		if lib.Xsqlite3_bind_parameter_count(c.tls, ptr) != 0 || s.ColumnCount() != 0 {
			return closeInstallStatement(s, misuse(PreparePhase))
		}
		row, stepErr := s.Step()
		if stepErr == nil && row {
			stepErr = misuse(StepPhase)
		}
		if err = closeInstallStatement(s, stepErr); err != nil {
			return err
		}
	}
	return t.check(PreparePhase)
}

// CheckForeignKeys audits the owned transaction without exposing a general
// PRAGMA path or returning potentially sensitive table/row information.
func (t *Tx) CheckForeignKeys() error {
	if err := t.check(VerifyPhase); err != nil {
		return err
	}
	if len(t.statements) != 0 {
		return misuse(VerifyPhase)
	}
	c := t.conn
	*(*uint32)(unsafe.Pointer(c.authMode)) = authPragma
	defer func() { *(*uint32)(unsafe.Pointer(c.authMode)) = authApplication }()
	s, err := t.Prepare("PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	if s.ColumnCount() != 4 {
		return closeInstallStatement(s, &Error{Phase: VerifyPhase, Category: Corrupt})
	}
	row, err := s.Step()
	if err == nil && row {
		err = &Error{Phase: VerifyPhase, Category: Corrupt}
	}
	return closeInstallStatement(s, err)
}

func closeInstallStatement(s *Stmt, cause error) error {
	if cleanup := s.Close(); cleanup != nil {
		if cause == nil {
			return cleanup
		}
		e := safeError(FinalizePhase, cause)
		e.Cleanup = joinCleanup(e.Cleanup, cleanup)
		return e
	}
	return cause
}
