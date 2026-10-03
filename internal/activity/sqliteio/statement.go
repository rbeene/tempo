//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"strings"
	"unsafe"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

type Kind uint8

const (
	NullKind Kind = iota
	IntegerKind
	TextKind
	BlobKind
)

type Value struct {
	kind    Kind
	integer int64
	text    string
	blob    []byte
}

func Null() Value           { return Value{kind: NullKind} }
func Integer(n int64) Value { return Value{kind: IntegerKind, integer: n} }
func Text(s string) Value   { return Value{kind: TextKind, text: s} }

// Blob(nil) is an empty BLOB. Null() is the sole SQL NULL constructor.
func Blob(b []byte) Value { return Value{kind: BlobKind, blob: b} }

type Stmt struct {
	tx        *Tx
	ptr       uintptr
	row, done bool
	stepErr   error
}

func (c *Conn) prepareRaw(sql string, phase Phase) (uintptr, error) {
	if sql == "" || strings.ContainsRune(sql, 0) || len(sql) >= 2147483647 {
		return 0, &Error{Phase: phase, Category: Invalid}
	}
	query, err := libc.CString(sql)
	if err != nil {
		return 0, safeError(phase, err)
	}
	defer libc.Xfree(c.tls, query)
	output := lib.Xsqlite3_malloc64(c.tls, 16)
	if output == 0 {
		return 0, engineError(phase, lib.SQLITE_NOMEM, nil)
	}
	defer lib.Xsqlite3_free(c.tls, output)
	*(*uintptr)(unsafe.Pointer(output)) = 0
	*(*uintptr)(unsafe.Pointer(output + 8)) = 0
	rc := lib.Xsqlite3_prepare_v2(c.tls, c.db, query, int32(len(sql)+1), output, output+8)
	stmt := *(*uintptr)(unsafe.Pointer(output))
	if rc != lib.SQLITE_OK {
		e := engineError(phase, rc, nil)
		if stmt != 0 {
			if f := lib.Xsqlite3_finalize(c.tls, stmt); f != lib.SQLITE_OK {
				e.Cleanup = engineError(FinalizePhase, f, nil)
			}
		}
		return 0, e
	}
	tailPtr := *(*uintptr)(unsafe.Pointer(output + 8))
	tail := ""
	if tailPtr != 0 {
		tail = libc.GoString(tailPtr)
	}
	if stmt == 0 || strings.TrimSpace(tail) != "" {
		e := &Error{Phase: phase, Category: Invalid}
		if stmt != 0 {
			if f := lib.Xsqlite3_finalize(c.tls, stmt); f != lib.SQLITE_OK {
				e.Cleanup = engineError(FinalizePhase, f, nil)
			}
		}
		return 0, e
	}
	return stmt, nil
}
func (t *Tx) Prepare(sql string, values ...Value) (_ *Stmt, err error) {
	defer func() { t.readFailure(err) }()
	if err := t.check(PreparePhase); err != nil {
		return nil, err
	}
	if err := sqlEvent(sqlTestEvent{Phase: "prepare-before", Operation: "prepare"}); err != nil {
		return nil, safeError(PreparePhase, err)
	}
	if err := t.check(PreparePhase); err != nil {
		return nil, err
	}
	c := t.conn
	ptr, err := c.prepareRaw(sql, PreparePhase)
	if err != nil {
		return nil, contextualError(PreparePhase, err, t.ctx)
	}
	s := &Stmt{tx: t, ptr: ptr}
	t.statements[s] = struct{}{}
	reject := func(cause error) (*Stmt, error) {
		e := safeError(PreparePhase, cause)
		if cleanup := s.Close(); cleanup != nil {
			e.Cleanup = joinCleanup(e.Cleanup, cleanup)
		}
		return nil, e
	}
	if t.mode == Read && lib.Xsqlite3_stmt_readonly(c.tls, ptr) == 0 {
		return reject(misuse(PreparePhase))
	}
	if int(lib.Xsqlite3_bind_parameter_count(c.tls, ptr)) != len(values) {
		return reject(misuse(BindPhase))
	}
	for i, v := range values {
		if err = s.bind(int32(i+1), v); err != nil {
			return reject(err)
		}
	}
	return s, nil
}
func (s *Stmt) bind(index int32, v Value) error {
	c := s.tx.conn
	var rc int32
	switch v.kind {
	case NullKind:
		rc = lib.Xsqlite3_bind_null(c.tls, s.ptr, index)
	case IntegerKind:
		rc = lib.Xsqlite3_bind_int64(c.tls, s.ptr, index, v.integer)
	case TextKind, BlobKind:
		data := v.blob
		if v.kind == TextKind {
			data = []byte(v.text)
		}
		// Non-NULL storage even for length zero; SQLite copies before return.
		p := lib.Xsqlite3_malloc64(c.tls, uint64(len(data))+1)
		if p == 0 {
			return engineError(BindPhase, lib.SQLITE_NOMEM, nil)
		}
		copy(unsafe.Slice((*byte)(unsafe.Pointer(p)), len(data)), data)
		*(*byte)(unsafe.Pointer(p + uintptr(len(data)))) = 0
		if v.kind == TextKind {
			rc = lib.Xsqlite3_bind_text64(c.tls, s.ptr, index, p, uint64(len(data)), ^uintptr(0), lib.SQLITE_UTF8)
		} else {
			rc = lib.Xsqlite3_bind_blob64(c.tls, s.ptr, index, p, uint64(len(data)), ^uintptr(0))
		}
		lib.Xsqlite3_free(c.tls, p)
	default:
		return misuse(BindPhase)
	}
	if rc != lib.SQLITE_OK {
		return engineError(BindPhase, rc, s.tx.ctx)
	}
	return nil
}
func (s *Stmt) Step() (_ bool, err error) {
	defer func() {
		if s != nil {
			s.tx.readFailure(err)
		}
	}()
	if s == nil || s.ptr == 0 {
		return false, safeError(StepPhase, ErrClosed)
	}
	if s.done {
		return false, s.stepErr
	}
	s.row = false
	if err := s.tx.check(StepPhase); err != nil {
		return false, err
	}
	if err := sqlEvent(sqlTestEvent{Phase: "step-before", Operation: "statement"}); err != nil {
		return false, safeError(StepPhase, err)
	}
	if err := s.tx.check(StepPhase); err != nil {
		return false, err
	}
	c := s.tx.conn
	observeSQL(sqlTestEvent{Phase: "step-before-native", Operation: "statement"})
	rc := lib.Xsqlite3_step(c.tls, s.ptr)
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) != 0 {
		s.tx.nativeLost = true
	}
	if rc != lib.SQLITE_ROW && rc != lib.SQLITE_DONE {
		err = engineError(StepPhase, rc, s.tx.ctx)
	}
	if fault := sqlEvent(sqlTestEvent{Phase: "step-after", Operation: "statement", Code: rc}); err == nil && fault != nil {
		err = safeError(StepPhase, fault)
	}
	if err == nil {
		if e := validateRoot(c.root); e != nil {
			err = safeError(VerifyPhase, e)
		}
	}
	if err != nil {
		s.done, s.stepErr = true, err
		return false, err
	}
	s.row = rc == lib.SQLITE_ROW
	s.done = rc == lib.SQLITE_DONE
	return s.row, nil
}
func (s *Stmt) ColumnCount() int {
	if s == nil || s.ptr == 0 {
		return 0
	}
	return int(lib.Xsqlite3_column_count(s.tx.conn.tls, s.ptr))
}
func (s *Stmt) Kind(index int) (_ Kind, err error) {
	defer func() {
		if s != nil {
			s.tx.readFailure(err)
		}
	}()
	if s == nil || s.ptr == 0 || !s.row || index < 0 || index >= s.ColumnCount() {
		return NullKind, misuse(StepPhase)
	}
	switch lib.Xsqlite3_column_type(s.tx.conn.tls, s.ptr, int32(index)) {
	case lib.SQLITE_NULL:
		return NullKind, nil
	case lib.SQLITE_INTEGER:
		return IntegerKind, nil
	case lib.SQLITE_TEXT:
		return TextKind, nil
	case lib.SQLITE_BLOB:
		return BlobKind, nil
	default:
		return NullKind, misuse(StepPhase)
	}
}
func (s *Stmt) expect(index int, want Kind) (err error) {
	defer func() {
		if s != nil {
			s.tx.readFailure(err)
		}
	}()
	kind, err := s.Kind(index)
	if err != nil {
		return err
	}
	if kind != want {
		return misuse(StepPhase)
	}
	return nil
}
func (s *Stmt) IsNull(index int) (bool, error) { k, err := s.Kind(index); return k == NullKind, err }
func (s *Stmt) Int64(index int) (int64, error) {
	if err := s.expect(index, IntegerKind); err != nil {
		return 0, err
	}
	return lib.Xsqlite3_column_int64(s.tx.conn.tls, s.ptr, int32(index)), nil
}
func (s *Stmt) bytes(index int, kind Kind) (_ []byte, err error) {
	defer func() {
		if s != nil {
			s.tx.readFailure(err)
		}
	}()
	if err := s.expect(index, kind); err != nil {
		return nil, err
	}
	c := s.tx.conn
	var p uintptr
	if kind == TextKind {
		p = lib.Xsqlite3_column_text(c.tls, s.ptr, int32(index))
	} else {
		p = lib.Xsqlite3_column_blob(c.tls, s.ptr, int32(index))
	}
	n := lib.Xsqlite3_column_bytes(c.tls, s.ptr, int32(index))
	if n < 0 || p == 0 && n != 0 {
		return nil, engineError(StepPhase, lib.SQLITE_NOMEM, nil)
	}
	if p == 0 && lib.Xsqlite3_errcode(c.tls, c.db) == lib.SQLITE_NOMEM {
		return nil, engineError(StepPhase, lib.SQLITE_NOMEM, nil)
	}
	result := make([]byte, int(n))
	if n != 0 {
		copy(result, unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n)))
	}
	return result, nil
}
func (s *Stmt) Text(index int) (string, error) {
	b, err := s.bytes(index, TextKind)
	return string(b), err
}
func (s *Stmt) Blob(index int) ([]byte, error) { return s.bytes(index, BlobKind) }
func (s *Stmt) Close() (err error) {
	defer func() {
		if s != nil {
			s.tx.readFailure(err)
		}
	}()
	if s == nil || s.ptr == 0 {
		return nil
	}
	c := s.tx.conn
	rc := lib.Xsqlite3_finalize(c.tls, s.ptr)
	s.ptr, s.row, s.done = 0, false, true
	delete(s.tx.statements, s)
	if rc != lib.SQLITE_OK {
		err = engineError(FinalizePhase, rc, s.tx.ctx)
	}
	if fault := sqlEvent(sqlTestEvent{Phase: "finalize-after", Operation: "statement", Code: rc}); fault != nil {
		if err == nil {
			err = safeError(FinalizePhase, fault)
		} else {
			e := safeError(FinalizePhase, err)
			e.Cleanup = joinCleanup(e.Cleanup, safeError(FinalizePhase, fault))
			err = e
		}
	}
	return err
}
