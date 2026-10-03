//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Test-first source against cb0b793e/9ebac6c. CloseChecked is a native API
// prerequisite at this freeze; no compiler or runtime result is claimed.

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

func TestSQLiteCloseCheckedNilSuccessAndLegacy(t *testing.T) {
	t.Run("nil-owner", func(t *testing.T) {
		trace := &freshQATrace{}
		qaFSHooks(t, hooks{Observe: trace.observeFS})
		qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
		var c *Conn
		terminal, err := c.CloseChecked(nil)
		if !terminal || err != nil || c.Close(nil) != nil {
			t.Fatal("nil owner changed checked or legacy close", terminal, err)
		}
		if len(trace.snapshot()) != 0 || trace.sqlCount("close-before", "close") != 0 {
			t.Fatal("nil owner dispatched cleanup")
		}
		freshQAQuiet(t)
	})
	for _, first := range []string{"checked", "legacy", "concurrent"} {
		t.Run(first, func(t *testing.T) {
			c := qaOpen(t, qaDirectory(t), qaBasename, true)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			switch first {
			case "checked":
				if terminal, err := c.CloseChecked(ctx); !terminal || err != nil {
					t.Fatal("ordinary checked close did not retire owner", terminal, err)
				}
			case "legacy":
				if err := c.Close(ctx); err != nil {
					t.Fatal("legacy ordinary close changed", err)
				}
			case "concurrent":
				type result struct {
					checked, terminal bool
					err               error
				}
				joined := make(chan result, 2)
				start := make(chan struct{})
				go func() {
					<-start
					terminal, err := c.CloseChecked(ctx)
					joined <- result{checked: true, terminal: terminal, err: err}
				}()
				go func() {
					<-start
					joined <- result{err: c.Close(ctx)}
				}()
				close(start)
				// Join both operations before any failure or hook/temp cleanup.
				a, b := <-joined, <-joined
				if a.err != nil || b.err != nil || a.checked && !a.terminal || b.checked && !b.terminal {
					t.Fatal("concurrent checked/legacy closes did not serialize", a, b)
				}
			}
			if terminal, err := c.CloseChecked(context.Background()); !terminal || err != nil {
				t.Fatal("terminal success changed on checked repeat", terminal, err)
			}
			if err := c.Close(context.Background()); err != nil {
				t.Fatal("terminal success changed on legacy repeat", err)
			}
			if trace.count("main", "close", "before") != 1 || trace.count("root", "lease", "released") != 1 || trace.sqlCount("close-before", "close") != 1 || trace.sqlCount("close-after", "close") != 1 {
				t.Fatal("terminal close dispatched native close/release more than once")
			}
			freshQAQuiet(t)
		})
	}
}

func TestSQLiteCloseCheckedNativeStatementBusyRetainsSameOwner(t *testing.T) {
	c := qaOpen(t, qaDirectory(t), qaBasename, true)
	release, err := c.holdStatementForTest("SELECT 1")
	if err != nil {
		t.Fatal("owned native statement prepare", err)
	}
	// This is an actual prepared native statement, with no Tx or watcher.
	// Its finalizer must run before the connection's registered cleanup.
	t.Cleanup(func() {
		if err := release(); err != nil {
			t.Error("owned native statement finalize cleanup", err)
		}
	})
	trace := &freshQATrace{}
	qaFSHooks(t, hooks{Observe: trace.observeFS})
	before, db := statsForTest(), c.db
	if before.Active != 1 || before.NativeActive != 1 || before.Entries != 1 || db == 0 {
		t.Fatal("native statement ownership premise", before)
	}
	terminal, err := c.CloseChecked(context.Background())
	var checked *Error
	if terminal || !errors.As(err, &checked) || checked.Phase != ClosePhase || checked.Category != Busy || checked.Code != lib.SQLITE_BUSY {
		t.Fatal("actual sqlite3_close BUSY did not retain owner", terminal, err)
	}
	qaSafeError(t, err, "SELECT 1")
	if statsForTest() != before || c.db != db || trace.count("root", "lease", "released") != 0 || trace.count("main", "close", "before") != 1 {
		t.Fatal("BUSY changed actual native/root ownership")
	}
	if err := release(); err != nil {
		t.Fatal("actual native statement finalize", err)
	}
	terminal, err = c.CloseChecked(context.Background())
	if !terminal || err != nil {
		t.Fatal("same-owner close after native finalize", terminal, err)
	}
	if trace.count("main", "close", "before") != 2 || trace.count("root", "lease", "released") != 1 {
		t.Fatal("BUSY/finalize/close native ownership sequence changed")
	}
	freshQAQuiet(t)
}

func TestSQLiteCloseCheckedRejectedContextAndOnceBeforeFault(t *testing.T) {
	t.Run("invalid-gate", func(t *testing.T) {
		terminal, err := (&Conn{}).CloseChecked(context.Background())
		var checked *Error
		if terminal || !errors.As(err, &checked) || checked.Phase != ClosePhase || checked.Category != Closed || !errors.Is(err, ErrClosed) {
			t.Fatal("invalid gate claimed proven terminal state", terminal, err)
		}
	})
	for _, rejection := range []string{"nil-context", "canceled-live", "held-gate-deadline", "canceled-terminal"} {
		t.Run(rejection, func(t *testing.T) {
			c := qaOpen(t, qaDirectory(t), qaBasename, true)
			if rejection == "canceled-terminal" {
				qaClose(t, c)
			}
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
			before := statsForTest()
			var ctx context.Context
			wantCategory := Invalid
			var wantCause error
			switch rejection {
			case "canceled-live", "canceled-terminal":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
				wantCategory, wantCause = Canceled, context.Canceled
			case "held-gate-deadline":
				if err := c.lockContext(context.Background(), time.Time{}); err != nil {
					t.Fatal("owned gate acquisition", err)
				}
				held := true
				defer func() {
					if held {
						c.unlock()
					}
				}()
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				wantCategory, wantCause = Canceled, context.DeadlineExceeded
				// While this same owned gate is held, only context expiry can
				// end acquisition. No sleeping worker or detached cleanup exists.
				terminal, err := c.CloseChecked(ctx)
				c.unlock()
				held = false
				var checked *Error
				if terminal || !errors.As(err, &checked) || checked.Phase != Admission || checked.Category != wantCategory || !errors.Is(err, wantCause) {
					t.Fatal("blocked gate did not reject original deadline", terminal, err)
				}
			}
			if rejection != "held-gate-deadline" {
				terminal, err := c.CloseChecked(ctx)
				var checked *Error
				if terminal || !errors.As(err, &checked) || checked.Phase != Admission || checked.Category != wantCategory || wantCause != nil && !errors.Is(err, wantCause) {
					t.Fatal("rejected context claimed observed terminal state", terminal, err)
				}
			}
			if statsForTest() != before || len(trace.snapshot()) != 0 || trace.sqlCount("close-before", "close") != 0 {
				t.Fatal("rejected context dispatched native close or changed ownership")
			}
			if terminal, err := c.CloseChecked(context.Background()); !terminal || err != nil {
				t.Fatal("valid cleanup could not retire/observe exact owner", terminal, err)
			}
			wantDispatch := 1
			if rejection == "canceled-terminal" {
				wantDispatch = 0
			}
			if trace.count("main", "close", "before") != wantDispatch || trace.count("root", "lease", "released") != wantDispatch {
				t.Fatal("later cleanup redispatched a previously terminal owner")
			}
			freshQAQuiet(t)
		})
	}
	t.Run("once-close-before", func(t *testing.T) {
		c := qaOpen(t, qaDirectory(t), qaBasename, true)
		trace := &freshQATrace{}
		qaFSHooks(t, hooks{Observe: trace.observeFS})
		reached := false
		qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL, Fault: func(e sqlTestEvent) error {
			if !reached && e.Phase == "close-before" && e.Operation == "close" {
				reached = true
				return unix.EIO
			}
			return nil
		}})
		before := statsForTest()
		terminal, err := c.CloseChecked(context.Background())
		var checked *Error
		if !reached || terminal || !errors.As(err, &checked) || checked.Phase != ClosePhase || checked.Category != IO || checked.Code != 0 {
			t.Fatal("once close-before fault lost retained ownership", terminal, err)
		}
		qaSafeError(t, err)
		if statsForTest() != before || trace.count("main", "close", "before") != 0 || trace.count("root", "lease", "released") != 0 {
			t.Fatal("preclose refusal dispatched native close/release")
		}
		if terminal, err := c.CloseChecked(context.Background()); !terminal || err != nil {
			t.Fatal("same retained owner did not close after once fault", terminal, err)
		}
		if trace.sqlCount("close-before", "close") != 2 || trace.count("main", "close", "before") != 1 || trace.count("root", "lease", "released") != 1 {
			t.Fatal("once preclose fault did not preserve dispatch order")
		}
		freshQAQuiet(t)
	})
}

func TestSQLiteCloseCheckedTerminalErrorsAreExactCachedWithoutRedispatch(t *testing.T) {
	for _, boundary := range []string{"ordinary-close-after", "durable-success", "durable-main-before", "durable-main-after"} {
		t.Run(boundary, func(t *testing.T) {
			dir := qaDirectory(t)
			c := freshQAInitial(t, dir) // Existing owner cleanup accepts expected cached failures.
			freshQASeed(t, c)           // Actual Write/DDL/row and successful COMMIT; Tx watcher joined.
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			reached := false
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL, Fault: func(e sqlTestEvent) error {
				match := boundary == "ordinary-close-after" && e.Phase == "close-after" && e.Operation == "close" ||
					boundary == "durable-main-before" && e.Phase == "fsync-before" && e.Operation == "durable-main" ||
					boundary == "durable-main-after" && e.Phase == "fsync-after" && e.Operation == "durable-main"
				if match && !reached {
					reached = true
					return unix.EIO
				}
				return nil
			}})
			var saved error
			if boundary == "ordinary-close-after" {
				terminal, err := c.CloseChecked(freshQAContext(t))
				if !terminal {
					t.Fatal("close-after failure did not report terminal owner", err)
				}
				saved = err
			} else {
				saved = c.CloseDurably(freshQAContext(t)) // Never retried as a durable ACK.
				if trace.sqlCount("durable-native-closed", "close-durably") != 1 || trace.count("main", "fsync", "before") != 1 {
					t.Fatal("actual durable closed-file main barrier was not reached")
				}
				wantAfter := 1
				if boundary == "durable-main-before" {
					wantAfter = 0
				}
				if trace.count("main", "fsync", "after") != wantAfter {
					t.Fatal("durable fault did not occur at selected actual fsync boundary")
				}
			}
			if boundary == "durable-success" {
				if saved != nil || reached {
					t.Fatal("positive durable barrier calibration failed", saved)
				}
			} else {
				var checked *Error
				if !reached || !errors.As(saved, &checked) || checked.Phase != ClosePhase || checked.Category != IO || checked.Code != 0 {
					t.Fatal("selected terminal failure lost original safe evidence", saved)
				}
				qaSafeError(t, saved, dir, freshQAValue)
			}
			fsCount := len(trace.snapshot())
			for repeat := 0; repeat < 2; repeat++ {
				terminal, err := c.CloseChecked(context.Background())
				if !terminal || err != saved {
					t.Fatal("checked close changed exact cached terminal error", terminal, err, saved)
				}
				if err := c.Close(context.Background()); err != saved {
					t.Fatal("legacy close changed exact cached terminal error", err, saved)
				}
			}
			if len(trace.snapshot()) != fsCount || trace.count("main", "close", "before") != 1 || trace.count("root", "lease", "released") != 1 || trace.sqlCount("close-before", "close") != 1 {
				t.Fatal("cached terminal outcome redispatched native close/barrier/release")
			}
			setSQLHooksForTest(sqlTestHooks{})
			setHooksForTest(hooks{})
			// These selected EIO hook cases have conclusive native/lease release;
			// terminal alone makes no claim about arbitrary OS-close failures.
			freshQAQuiet(t)
			if freshQARows(t, dir) != 1 {
				t.Fatal("terminal cleanup outcome erased actual committed receipt")
			}
		})
	}
}
