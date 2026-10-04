//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	lib "modernc.org/sqlite/lib"
)

type swQAOwner struct {
	c        *Conn
	tx       *Tx
	ended    bool
	closeErr error
}

func (o *swQAOwner) close() error {
	if o.ended {
		return o.closeErr
	}
	var err error
	if o.tx != nil {
		err = o.tx.Rollback()
	}
	if o.c != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		terminal, closeErr := o.c.CloseChecked(ctx)
		err = errors.Join(err, closeErr)
		if !terminal {
			terminal, closeErr = o.c.CloseChecked(ctx)
			err = errors.Join(err, closeErr)
		}
		if !terminal {
			err = errors.Join(err, errors.New("native fixture owner remains nonterminal"))
		}
		o.ended = terminal
	} else {
		o.ended = true
	}
	o.closeErr = errors.Join(o.closeErr, err)
	return o.closeErr
}

func swQAOpen(t *testing.T, ctx context.Context, mode Mode) (*swQAOwner, string) {
	t.Helper()
	base := qaDirectory(t)
	dir := filepath.Join(base, "store")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	c, err := Open(ctx, dir, qaBasename, Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	o := &swQAOwner{c: c}
	t.Cleanup(func() {
		if err := o.close(); err != nil {
			t.Error("checked native fixture cleanup", err)
		}
	})
	if err == nil {
		o.tx, err = c.Begin(ctx, mode)
	}
	if err != nil {
		t.Fatal("native fixture acquisition", errors.Join(err, o.close()))
	}
	return o, dir
}

// All measured passes execute the same actual Prepare, typed ROW, DONE and
// checked finalization on one live native transaction; no hook fabricates data.
func swQASelect(tx *Tx) (err error) {
	s, err := tx.Prepare("SELECT 7")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	row, err := s.Step()
	if err != nil {
		return err
	}
	if !row || s.ColumnCount() != 1 {
		return errors.New("native SELECT ROW/width control")
	}
	kind, err := s.Kind(0)
	if err != nil {
		return err
	}
	if kind != IntegerKind {
		return errors.New("native SELECT INTEGER control")
	}
	value, err := s.Int64(0)
	if err != nil {
		return err
	}
	if value != 7 {
		return errors.New("native SELECT value control")
	}
	row, err = s.Step()
	if err != nil {
		return err
	}
	if row {
		return errors.New("native SELECT DONE control")
	}
	return nil
}

func TestSQLiteStatementEmptyHooksReduceCalibratedNativeWork(t *testing.T) {
	for _, mode := range []Mode{Read, Write} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			setSQLHooksForTest(sqlTestHooks{})
			t.Cleanup(func() { setSQLHooksForTest(sqlTestHooks{}) })
			o, _ := swQAOpen(t, context.Background(), mode)
			if err := swQASelect(o.tx); err != nil {
				t.Fatal("native positive control", err)
			}
			const runs = 20
			var failure error
			walk := testing.AllocsPerRun(runs, func() {
				if err := validateRoot(o.c.root); err != nil && failure == nil {
					failure = err
				}
			})
			if failure != nil || walk <= 0 {
				t.Fatal("actual same-root walk allocation calibration", walk, failure)
			}
			measure := func(h sqlTestHooks) float64 {
				setSQLHooksForTest(h)
				n := testing.AllocsPerRun(runs, func() {
					if err := swQASelect(o.tx); err != nil && failure == nil {
						failure = err
					}
				})
				setSQLHooksForTest(sqlTestHooks{})
				if failure != nil {
					t.Fatal("measured native loop failed", failure)
				}
				return n
			}
			plain := measure(sqlTestHooks{})
			observeCalls := 0
			observed := measure(sqlTestHooks{Observe: func(e sqlTestEvent) {
				if e.Phase == "prepare-before" || e.Phase == "step-before" {
					observeCalls++
				}
			}})
			faultCalls := 0
			faulted := measure(sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if e.Phase == "prepare-before" || e.Phase == "step-before" {
					faultCalls++
				}
				return nil
			}})
			if observeCalls != 3*(runs+1) || faultCalls != 3*(runs+1) {
				t.Fatal("real Observe/Fault controls did not cover Prepare/ROW/DONE", observeCalls, faultCalls)
			}
			if err := o.close(); err != nil {
				t.Fatal("measured transaction checked close", err)
			}
			t.Logf("actual native allocations plain=%v observe=%v fault=%v oneRootWalk=%v", plain, observed, faulted, walk)
			// The candidate removes three redundant walks per complete pass.
			// Requiring savings of at least two actual walks rejects a trivial
			// allocation fluctuation; this is no wall-clock budget assertion.
			if observed-plain < 2*walk || faulted-plain < 2*walk {
				t.Fatal("empty hooks did not remove calibrated redundant root-walk work")
			}
		})
	}
}

func TestSQLiteStatementBeforeEventUsesOneCapturedHookSnapshot(t *testing.T) {
	for _, operation := range []string{"prepare", "step"} {
		t.Run(operation, func(t *testing.T) {
			setSQLHooksForTest(sqlTestHooks{})
			t.Cleanup(func() { setSQLHooksForTest(sqlTestHooks{}) })
			o, _ := swQAOpen(t, context.Background(), Read)
			var s *Stmt
			var err error
			if operation == "step" {
				s, err = o.tx.Prepare("SELECT 7")
				if err != nil {
					t.Fatal(err)
				}
			}
			phase := operation + "-before"
			observed, oldFault, replacementFault := 0, 0, 0
			replacement := sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if e.Phase == phase {
					replacementFault++
					return &Error{Phase: Phase(operation), Category: IO, Code: 778}
				}
				return nil
			}}
			setSQLHooksForTest(sqlTestHooks{
				Observe: func(e sqlTestEvent) {
					if e.Phase == phase {
						observed++
						setSQLHooksForTest(replacement)
					}
				},
				Fault: func(e sqlTestEvent) error {
					if e.Phase == phase {
						oldFault++
						return &Error{Phase: Phase(operation), Category: IO, Code: 266}
					}
					return nil
				},
			})
			var row bool
			var unexpected *Stmt
			if operation == "prepare" {
				unexpected, err = o.tx.Prepare("SELECT 7")
			} else {
				row, err = s.Step()
			}
			var native *Error
			if unexpected != nil || row || !errors.As(err, &native) || native.Code != 266 ||
				observed != 1 || oldFault != 1 || replacementFault != 0 || !o.tx.readFailed {
				t.Fatal("callback replacement mixed hook snapshots or lost refusal", err, observed, oldFault, replacementFault)
			}
			if s != nil && lib.Xsqlite3_stmt_status(o.c.tls, s.ptr, lib.SQLITE_STMTSTATUS_RUN, 0) != 0 {
				t.Fatal("captured fault still dispatched native Step")
			}
			// A later event must see the replacement; do not permanently cache it.
			if operation == "prepare" {
				unexpected, err = o.tx.Prepare("SELECT 7")
			} else {
				row, err = s.Step()
			}
			if unexpected != nil || row || !errors.As(err, &native) || native.Code != 778 || replacementFault != 1 {
				t.Fatal("later event did not observe newly installed hook", err, replacementFault)
			}
			setSQLHooksForTest(sqlTestHooks{})
			if s != nil {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := o.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSQLiteStatementRootAndContextGuardsBracketInstalledCallbacks(t *testing.T) {
	for _, operation := range []string{"prepare", "step"} {
		for _, scenario := range []string{"empty-root", "before-root", "callback-root", "callback-cancel", "empty-cancel"} {
			t.Run(operation+"/"+scenario, func(t *testing.T) {
				setSQLHooksForTest(sqlTestHooks{})
				t.Cleanup(func() { setSQLHooksForTest(sqlTestHooks{}) })
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				o, dir := swQAOpen(t, ctx, Read)
				var s *Stmt
				var err error
				if operation == "step" {
					s, err = o.tx.Prepare("SELECT 7")
					if err != nil {
						t.Fatal(err)
					}
				}
				moved := dir + "-displaced"
				displaced, replacement := false, false
				restore := func() error {
					if replacement {
						if e := os.Remove(dir); e != nil {
							return e
						}
						replacement = false
					}
					if displaced {
						if e := os.Rename(moved, dir); e != nil {
							return e
						}
						displaced = false
					}
					return nil
				}
				t.Cleanup(func() {
					setSQLHooksForTest(sqlTestHooks{})
					if err := restore(); err != nil {
						t.Error("checked namespace restoration", err)
					}
				})
				swap := func() error {
					if e := os.Rename(dir, moved); e != nil {
						return e
					}
					displaced = true
					if e := os.Mkdir(dir, 0700); e != nil {
						return e
					}
					replacement = true
					return nil
				}
				phase := operation + "-before"
				calls := 0
				var mutationErr error
				if scenario == "before-root" || scenario == "callback-root" {
					setSQLHooksForTest(sqlTestHooks{Observe: func(e sqlTestEvent) {
						if e.Phase == phase {
							calls++
							if scenario == "callback-root" {
								mutationErr = swap()
							}
						}
					}})
				} else if scenario == "callback-cancel" {
					setSQLHooksForTest(sqlTestHooks{Fault: func(e sqlTestEvent) error {
						if e.Phase == phase {
							calls++
							cancel()
						}
						return nil
					}})
				}
				if scenario == "before-root" || scenario == "empty-root" {
					if err := swap(); err != nil {
						t.Fatal("real namespace control", err)
					}
				}
				if scenario == "empty-cancel" {
					cancel()
				}
				var row bool
				var unexpected *Stmt
				if operation == "prepare" {
					unexpected, err = o.tx.Prepare("SELECT 7")
				} else {
					row, err = s.Step()
				}
				wantCalls := 0
				if scenario == "callback-root" || scenario == "callback-cancel" {
					wantCalls = 1
				}
				var native *Error
				wantCategory := Unsafe
				if scenario == "callback-cancel" || scenario == "empty-cancel" {
					wantCategory = Canceled
				}
				if mutationErr != nil || unexpected != nil || row || calls != wantCalls || !errors.As(err, &native) || native.Category != wantCategory || !o.tx.readFailed {
					t.Fatal("root/context preflight or callback ordering changed", err, mutationErr, calls)
				}
				if wantCategory == Canceled && !errors.Is(err, context.Canceled) {
					t.Fatal("lost cancellation identity", err)
				}
				if s != nil && (s.row || lib.Xsqlite3_stmt_status(o.c.tls, s.ptr, lib.SQLITE_STMTSTATUS_RUN, 0) != 0) {
					t.Fatal("refused pre-native Step dispatched or exposed a row")
				}
				setSQLHooksForTest(sqlTestHooks{})
				if err := restore(); err != nil {
					t.Fatal(err)
				}
				if s != nil {
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if err := o.close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestSQLiteStatementPostNativeRootFailureRetainsCachedEvidence(t *testing.T) {
	setSQLHooksForTest(sqlTestHooks{})
	t.Cleanup(func() { setSQLHooksForTest(sqlTestHooks{}) })
	o, dir := swQAOpen(t, context.Background(), Read)
	s, err := o.tx.Prepare("SELECT 7")
	if err != nil {
		t.Fatal(err)
	}
	moved := dir + "-displaced"
	displaced, replacement := false, false
	restore := func() error {
		if replacement {
			if err := os.Remove(dir); err != nil {
				return err
			}
			replacement = false
		}
		if displaced {
			if err := os.Rename(moved, dir); err != nil {
				return err
			}
			displaced = false
		}
		return nil
	}
	t.Cleanup(func() {
		setSQLHooksForTest(sqlTestHooks{})
		if err := restore(); err != nil {
			t.Error(err)
		}
	})
	calls := 0
	var swapErr error
	setSQLHooksForTest(sqlTestHooks{Observe: func(e sqlTestEvent) {
		if e.Phase == "step-after" && e.Code == lib.SQLITE_ROW {
			calls++
			swapErr = os.Rename(dir, moved)
			if swapErr == nil {
				displaced = true
				swapErr = os.Mkdir(dir, 0700)
				replacement = swapErr == nil
			}
		}
	}})
	row, err := s.Step()
	var native *Error
	if swapErr != nil || calls != 1 || row || !errors.As(err, &native) || native.Category != Unsafe ||
		!o.tx.readFailed || !s.done || s.row || lib.Xsqlite3_stmt_status(o.c.tls, s.ptr, lib.SQLITE_STMTSTATUS_RUN, 0) != 1 {
		t.Fatal("actual native ROW was exposed after namespace replacement", err, swapErr, calls)
	}
	if again, cached := s.Step(); again || cached != err || calls != 1 {
		t.Fatal("failed completed Step lost cached evidence or redispatched", cached, calls)
	}
	setSQLHooksForTest(sqlTestHooks{})
	if err := restore(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
}
