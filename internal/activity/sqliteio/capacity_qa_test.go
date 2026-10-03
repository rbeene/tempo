//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	lib "modernc.org/sqlite/lib"
)

func TestSQLiteCapacityFixedPolicyAndRealPageFullRollback(t *testing.T) {
	dir := qaDirectory(t)
	capacityQAInitialize(t, dir, 8)
	c := capacityQAOpen(t, dir, false, 8)
	tx := qaBegin(t, c, capacityQAContext(t), Write)
	pages, err := tx.PageInfo()
	if err != nil || pages.PageSize != 4096 || pages.MaxPages != 8 || pages.JournalLimitBytes != 4<<20 {
		t.Fatalf("actual private page policy=%+v error=%v", pages, err)
	}
	qaDone(t, tx, "INSERT INTO capacity_items(id,body) VALUES(2,?)", Blob([]byte("staged-before-full")))
	if n := capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items WHERE id=2"); n != 1 {
		t.Fatal("fixture did not stage an earlier row")
	}
	s := qaPrepare(t, tx, "INSERT INTO capacity_items(id,body) VALUES(3,?)", Blob(bytes.Repeat([]byte{0x71}, 128<<10)))
	row, stepErr := s.Step()
	closeErr := s.Close()
	var checked *Error
	if row || !errors.As(stepErr, &checked) || checked.Category != Full || checked.Code&255 != lib.SQLITE_FULL {
		t.Fatalf("real small-page limit did not produce native FULL: row=%t error=%v", row, stepErr)
	}
	if closeErr != nil {
		var finalized *Error
		if !errors.As(closeErr, &finalized) || finalized.Code&255 != lib.SQLITE_FULL {
			t.Errorf("finalize lost original FULL evidence: %v", closeErr)
		}
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if outcome, _ := tx.Commit(); outcome != NotCommitted {
		t.Errorf("FULL rollback outcome=%v", outcome)
	}
	qaClose(t, c)
	c = capacityQAOpen(t, dir, false, 8)
	tx = qaBegin(t, c, capacityQAContext(t), Write)
	pages, err = tx.PageInfo()
	if err != nil || pages.MaxPages != 8 || pages.PageCount > 8 {
		t.Fatalf("reopened page ceiling=%+v error=%v", pages, err)
	}
	if n := capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items"); n != 1 {
		t.Fatalf("FULL retained partial staged data: count=%d", n)
	}
	qaDone(t, tx, "INSERT INTO capacity_items(id,body) VALUES(4,?)", Blob([]byte("later-valid-write")))
	qaCommit(t, tx)
	qaClose(t, c)
	if n := capacityQACount(t, dir); n != 2 {
		t.Fatalf("earlier and later acknowledgments not durable: count=%d", n)
	}
}

func TestSQLiteCapacityPrivateCeilingValidationAndProductionDefault(t *testing.T) {
	for _, invalid := range []int64{-1, 65537} {
		t.Run(map[int64]string{-1: "negative", 65537: "above-production"}[invalid], func(t *testing.T) {
			dir := qaDirectory(t)
			var mu sync.Mutex
			opens := 0
			qaFSHooks(t, hooks{Observe: func(e event) {
				if e.Op == "open" && e.Phase == "before" {
					mu.Lock()
					opens++
					mu.Unlock()
				}
			}})
			c, err := Open(capacityQAContext(t), dir, capacityQAName, Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond), testMaxPages: invalid})
			if c != nil {
				qaClose(t, c)
			}
			var checked *Error
			if !errors.As(err, &checked) || checked.Category != Invalid {
				t.Fatalf("invalid private ceiling: %v", err)
			}
			entries, readErr := os.ReadDir(dir)
			mu.Lock()
			gotOpens := opens
			mu.Unlock()
			if readErr != nil || len(entries) != 0 || gotOpens != 0 {
				t.Fatalf("invalid ceiling performed file work: entries=%d opens=%d error=%v", len(entries), gotOpens, readErr)
			}
		})
	}
	dir := qaDirectory(t)
	capacityQAInitialize(t, dir, 0)
	c := capacityQAOpen(t, dir, false, 0)
	tx := qaBegin(t, c, capacityQAContext(t), Read)
	p, err := tx.PageInfo()
	if err != nil || p.PageSize != 4096 || p.MaxPages != 65536 || p.JournalLimitBytes != 4194304 {
		t.Fatalf("production fixed policy=%+v error=%v", p, err)
	}
	qaCommit(t, tx)
	qaClose(t, c)
}

func TestSQLiteCapacityOversizeRemainsReadableAndCheckpointable(t *testing.T) {
	dir := qaDirectory(t)
	capacityQAInitialize(t, dir, 64)
	capacityQAAppend(t, dir, 2, 32<<10)
	c := capacityQAOpen(t, dir, false, 2)
	if tx, err := c.Begin(capacityQAContext(t), Write); err == nil || tx != nil {
		if tx != nil {
			_ = tx.Rollback()
		}
		t.Fatal("oversize database admitted a semantic writer")
	} else {
		var checked *Error
		if !errors.As(err, &checked) || checked.Category != Full || checked.Code != 0 {
			t.Errorf("oversize admission lost capacity category: %v", err)
		}
	}
	tx := qaBegin(t, c, capacityQAContext(t), Read)
	p, err := tx.PageInfo()
	if err != nil || p.PageCount <= 2 || p.MaxPages < p.PageCount {
		t.Fatalf("oversize was hidden or claimed reduced: %+v error=%v", p, err)
	}
	if n := capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items"); n != 2 {
		t.Fatal("oversize read lost retained rows")
	}
	qaCommit(t, tx)
	r, err := c.Checkpoint(capacityQAContext(t), Truncate)
	if err != nil || !r.Attempted || r.Code != lib.SQLITE_OK || !r.AfterValid {
		t.Fatalf("oversize checkpoint-only usability: %+v error=%v", r, err)
	}
	qaClose(t, c)
	if n := capacityQACount(t, dir); n != 2 {
		t.Fatal("oversize maintenance changed domain rows")
	}
}

func TestSQLiteCapacityExistingWrongPageSizeNeverRewritten(t *testing.T) {
	for _, create := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing", true: "create-does-not-prove-fresh"}[create], func(t *testing.T) {
			dir := qaDirectory(t)
			qaCreate8192PageFixture(t, dir, capacityQAName)
			path := filepath.Join(dir, capacityQAName)
			before, err := os.ReadFile(path) // Fixture connection is conclusively closed.
			if err != nil || len(before) < 100 || binary.BigEndian.Uint16(before[16:18]) != 8192 {
				t.Fatalf("fixture lacks actual 8192-byte main header: bytes=%d error=%v", len(before), err)
			}
			c, openErr := Open(capacityQAContext(t), dir, capacityQAName, Options{Create: create, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			if c != nil {
				qaClose(t, c)
			}
			if openErr == nil {
				t.Fatal("existing incompatible page size admitted")
			}
			qaSafeError(t, openErr, dir, "incompatible_page_fixture")
			after, err := os.ReadFile(path) // No native connection remains.
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("refusal rewrote incompatible main database")
			}
		})
	}
}

func TestSQLiteCapacityFootprintUsesPinnedStatsAndPreservesMainLock(t *testing.T) {
	dir := qaDirectory(t)
	capacityQAInitialize(t, dir, 0)
	capacityQAAppend(t, dir, 2, 16<<10)
	c := capacityQAOpen(t, dir, false, 0)
	tx := qaBegin(t, c, capacityQAContext(t), Read)
	_ = capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items")
	capacityQALockWitness(t, dir)
	var mu sync.Mutex
	mainOpens := 0
	roleStats := make(map[string]int)
	qaFSHooks(t, hooks{Observe: func(e event) {
		mu.Lock()
		defer mu.Unlock()
		if e.Role == "main" && e.Op == "open" && e.Phase == "before" {
			mainOpens++
		}
		if e.Op == "footprint" && e.Phase == "before" {
			roleStats[e.Role]++
		}
	}})
	for i := 0; i < 4; i++ {
		got, err := tx.Footprint()
		if err != nil {
			t.Fatal(err)
		}
		var want Footprint
		for suffix, output := range map[string]*int64{"": &want.Main, "-wal": &want.WAL, "-shm": &want.SHM, "-journal": &want.Journal} {
			info, err := os.Lstat(filepath.Join(dir, capacityQAName+suffix))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil || !info.Mode().IsRegular() || info.Size() < 0 {
				t.Fatalf("footprint fixture stat error=%v", err)
			}
			*output = info.Size()
			want.Total += info.Size()
		}
		if got != want || got.WAL == 0 {
			t.Fatalf("pinned footprint=%+v independent lstat=%+v", got, want)
		}
	}
	mu.Lock()
	gotOpens := mainOpens
	for _, role := range []string{"main", "wal", "shm", "journal"} {
		if roleStats[role] != 4 {
			t.Errorf("footprint role %s had %d actual role observations; expected four", role, roleStats[role])
		}
	}
	if len(roleStats) != 4 {
		t.Errorf("footprint enumerated %d distinct roles", len(roleStats))
	}
	mu.Unlock()
	if gotOpens != 0 {
		t.Errorf("footprint reopened main through VFS %d times", gotOpens)
	}
	// A hidden raw open/close would release this process's POSIX main locks,
	// even if it bypassed the VFS observer. The fresh child verifies ownership.
	capacityQALockWitness(t, dir)
	qaCommit(t, tx)
	qaClose(t, c)
}

func TestSQLiteCapacityFootprintArithmeticRefusesOverflowAndNegative(t *testing.T) {
	for _, input := range []Footprint{
		{Main: -1}, {WAL: -1}, {SHM: -1}, {Journal: -1},
		{Main: math.MaxInt64, WAL: 1}, {WAL: math.MaxInt64, SHM: 1},
		{Main: math.MaxInt64 - 2, WAL: 1, SHM: 1, Journal: 1},
	} {
		if err := input.sum(); err == nil {
			t.Errorf("invalid observed-length sum was accepted: %+v", input)
		}
	}
	for _, input := range []Footprint{{}, {Main: 4096, WAL: 32768, SHM: 32768, Journal: 13}, {Main: math.MaxInt64 - 3, WAL: 1, SHM: 1, Journal: 1}} {
		want := input.Main + input.WAL + input.SHM + input.Journal
		if err := input.sum(); err != nil || input.Total != want {
			t.Errorf("valid observed-length sum=%+v want=%d error=%v", input, want, err)
		}
	}
}

func TestSQLiteCapacityFootprintRefusesUnsafeOptionalRole(t *testing.T) {
	dir := qaDirectory(t)
	capacityQAInitialize(t, dir, 0)
	c := capacityQAOpen(t, dir, false, 0)
	tx := qaBegin(t, c, capacityQAContext(t), Read)
	_ = capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items")
	outside := filepath.Join(qaDirectory(t), "outside-sentinel")
	if err := os.WriteFile(outside, []byte("outside-unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	role := filepath.Join(dir, capacityQAName+"-journal")
	if err := os.Symlink(outside, role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(role) })
	if _, err := tx.Footprint(); err == nil {
		t.Fatal("footprint accepted an unowned symlink role")
	} else {
		qaSafeError(t, err, outside, "outside-unchanged")
	}
	if err := os.Remove(role); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "outside-unchanged" {
		t.Fatal("footprint changed outside target")
	}
}

func TestSQLiteCapacityOrdinaryLastCloseDoesNotBackfillWAL(t *testing.T) {
	dir := qaDirectory(t)
	capacityQAInitialize(t, dir, 0)
	capacityQAAppend(t, dir, 2, 128<<10)
	// No native connection or helper is live at these raw byte-oracle reads.
	if statsForTest().Active != 0 {
		t.Fatal("raw closed-file oracle requires zero live adapter connections")
	}
	path := filepath.Join(dir, capacityQAName)
	before, err := os.ReadFile(path)
	walBefore, walErr := os.Stat(path + "-wal")
	if err != nil || walErr != nil || walBefore.Size() == 0 {
		t.Fatal("fixture lacks outstanding committed WAL")
	}
	if n := capacityQACount(t, dir); n != 2 {
		t.Fatal("ordinary read lost committed WAL content")
	}
	after, err := os.ReadFile(path)
	walAfter, walErr := os.Stat(path + "-wal")
	if err != nil || walErr != nil || sha256.Sum256(before) != sha256.Sum256(after) || walAfter.Size() != walBefore.Size() {
		t.Fatal("ordinary read/last-close backfilled or reclaimed outstanding WAL")
	}
	c := capacityQAOpen(t, dir, false, 0)
	r, err := c.Checkpoint(capacityQAContext(t), Truncate)
	if err != nil || !r.Attempted || !r.AfterValid || r.After.WAL != 0 {
		t.Fatalf("explicit checkpoint did not reclaim WAL: %+v error=%v", r, err)
	}
	qaClose(t, c)
	afterExplicit, err := os.ReadFile(path)
	if err != nil || sha256.Sum256(before) == sha256.Sum256(afterExplicit) {
		t.Fatal("explicit checkpoint lacks actual main-file backfill witness")
	}
}

func TestSQLiteCapacityAbsentNonCreateOpenCreatesNothing(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		dir := qaDirectory(t)
		c, err := Open(context.Background(), dir, capacityQAName, Options{ReadOnly: readOnly, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		if c != nil {
			qaClose(t, c)
		}
		if err == nil {
			t.Fatal("absent non-create open unexpectedly succeeded")
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("non-create inspection created entries=%d error=%v", len(entries), err)
		}
	}
	// Service.Maintain's absent-state result is a separate consumer obligation;
	// this ready adapter test does not claim that unimplemented API is covered.
}
