//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// The observer controls use public Link and Status, an independent checked
// native owner and the literal 32-table audit. They never retry Status.
type sodQAAudit struct {
	meta sqliteStoreMeta
	rows map[string][][]string
}

func sodQALinked(t *testing.T) *stQAFixture {
	t.Helper()
	q := &stQAFixture{t: t, f: interopLocation(t)}
	q.service = NewSQLite(Options{Path: filepath.Join(q.f.directory, q.f.authority), Clock: ClockFunc(func() (ClockSample, error) { return stQAAt(0), nil })})
	r, err := q.service.Link(context.Background(), qaLinkInput(t), qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil || !r.Changed || r.SnapshotRevision != "1" {
		t.Fatal("SETUP actual public SQLite Link", err)
	}
	return q
}

func sodQARead(t *testing.T, f interopFixture) sodQAAudit {
	t.Helper()
	o := mwQAOpen(t, f, sqliteio.Read)
	defer o.cleanup()
	m, err := sqliteReadCaptureSchema(o.tx, f.authority, f.database)
	if err != nil {
		t.Fatal("cold exact catalog and metadata", err)
	}
	rows, charge := sgQAAudit(t, o.tx, m)
	if len(rows) != 32 || charge != m.LogicalBytes {
		t.Fatal("cold literal32 and independently recomputed charge")
	}
	stQAClose(t, o, false)
	return sodQAAudit{m, rows}
}

type sodQAFaults struct {
	name                                                string
	cancel                                              context.CancelFunc
	primary, cleanup                                    error
	primaryHits, cleanupHits, cancelHits                atomic.Int64
	roots, releases, nativeSetups, nativeBegins, closes atomic.Int64
}

func (f *sodQAFaults) install(t *testing.T) {
	t.Helper()
	ncQASetFSHooks(ncQAFSHooks{Observe: func(e ncQAFSEvent) {
		if e.Op == "lease" && e.Role == "root" {
			if e.Phase == "acquired" {
				f.roots.Add(1)
			}
			if e.Phase == "released" {
				f.releases.Add(1)
			}
		}
	}})
	spQASetSQLHooks(spQASQLHooks{
		Observe: func(e spQASQLEvent) {
			if e.Phase == "control-before-native" && e.Operation == "setup" {
				f.nativeSetups.Add(1)
				if f.name == "inspection-cancel" && f.cancelHits.CompareAndSwap(0, 1) {
					f.cancel()
				}
			}
			if e.Phase == "control-before-native" && e.Operation == "begin" {
				f.nativeBegins.Add(1)
				if f.name == "begin-cancel" && f.cancelHits.CompareAndSwap(0, 1) {
					f.cancel()
				}
			}
			if e.Phase == "close-after" && e.Operation == "close" {
				f.closes.Add(1)
			}
		},
		Fault: func(e spQASQLEvent) error {
			if (f.name == "schema-error" || f.name == "primary-cleanup") && e.Phase == "prepare-before" && e.Operation == "prepare" && f.primaryHits.CompareAndSwap(0, 1) {
				return &sqliteio.Error{Phase: sqliteio.PreparePhase, Category: sqliteio.IO, Code: 10, Cause: f.primary}
			}
			if (f.name == "cleanup-error" || f.name == "primary-cleanup") && e.Phase == "close-before" && e.Operation == "close" && f.cleanupHits.CompareAndSwap(0, 1) {
				return &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Code: 10, Cause: f.cleanup}
			}
			return nil
		},
	})
	t.Cleanup(sodQAClear)
}

func sodQAClear() {
	spQASetSQLHooks(spQASQLHooks{})
	ncQASetFSHooks(ncQAFSHooks{})
}

func (f *sodQAFaults) check(t *testing.T, err error) {
	t.Helper()
	if f.roots.Load() != f.releases.Load() {
		t.Fatal("Status did not release every witnessed root owner")
	}
	primary := f.name == "schema-error" || f.name == "primary-cleanup"
	cleanup := f.name == "cleanup-error" || f.name == "primary-cleanup"
	if (f.primaryHits.Load() == 1) != primary || errors.Is(err, f.primary) != primary || (f.cleanupHits.Load() == 1) != cleanup || errors.Is(err, f.cleanup) != cleanup {
		t.Fatal("original primary or checked cleanup cause lost, duplicated or invented")
	}
	if (f.cancelHits.Load() == 1) != (f.name == "inspection-cancel" || f.name == "begin-cancel") {
		t.Fatal("deterministic existing native boundary was not reached exactly once")
	}
}

func sodQAUnchanged(t *testing.T, f interopFixture, before sodQAAudit) {
	t.Helper()
	if !reflect.DeepEqual(before, sodQARead(t, f)) {
		t.Fatal("Status changed exact32 rows, metadata, nonce, identity or logical charge")
	}
}
