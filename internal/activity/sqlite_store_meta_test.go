//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Private typed-metadata and arithmetic fixtures, not full-table accounting acceptance.

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func metaQARead(t *testing.T, f interopFixture) sqliteStoreMeta {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	m, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil {
		t.Fatal(err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	return m
}
func metaQANext(t *testing.T, m sqliteStoreMeta) sqliteStoreMeta {
	t.Helper()
	n, err := sqliteNextNonce(m.DurabilityNonce[:])
	if err != nil {
		t.Fatal(err)
	}
	m.DurabilityNonce = n
	return m
}
func metaQACapacity(t *testing.T, err error) {
	t.Helper()
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != "validation" || domain.Details["reason"] != "logical_capacity" {
		t.Fatalf("missing fixed logical capacity reason: %v", err)
	}
}

func TestSQLiteRowChargeIndependentByteOracle(t *testing.T) {
	for _, tc := range []struct {
		name            string
		integers, nulls int
		texts           []string
		blobs           [][]byte
		want            int64
	}{
		{"empty", 0, 0, nil, nil, 32}, {"integer", 1, 0, nil, nil, 41}, {"null", 0, 1, nil, nil, 33},
		{"empty_text", 0, 0, []string{""}, nil, 41}, {"empty_blob", 0, 0, nil, [][]byte{nil}, 41},
		{"utf8", 0, 0, []string{"雪"}, nil, 44},
		{"mixed", 2, 1, []string{"abc", "雪🙂"}, [][]byte{{0, 1, 2}}, 91},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sqliteRowCharge(tc.integers, tc.nulls, tc.texts, tc.blobs)
			if err != nil || got != tc.want {
				t.Fatalf("charge=%d want=%d error=%v", got, tc.want, err)
			}
		})
	}
	for _, counts := range [][2]int{{-1, 0}, {0, -1}, {math.MaxInt, 0}, {math.MaxInt, math.MaxInt}} {
		if _, err := sqliteRowCharge(counts[0], counts[1], nil, nil); err == nil {
			t.Fatal("negative/overflow charge admitted")
		}
	}
	f := interopLocation(t)
	m := interopMeta(f)
	want := int64(114 + len(interopComputer) + len(f.authority) + len(f.database))
	got, err := sqliteMetaCharge(m)
	if err != nil || got != want {
		t.Fatalf("explicit metadata charge=%d want=%d error=%v", got, want, err)
	}
	for i := range m.DurabilityNonce {
		m.DurabilityNonce[i] = 255
	}
	m.LogicalBytes = math.MaxInt64
	got, err = sqliteMetaCharge(m)
	if err != nil || got != want {
		t.Fatal("excluded nonce/counter changed metadata charge")
	}
	migration := "00000000-0000-4000-8000-000000000004"
	backup := strings.Repeat("b", 64)
	m.MigrationID = &migration
	m.BackupSHA256 = &backup
	got, err = sqliteMetaCharge(m)
	if err != nil || got != want+116 {
		t.Fatalf("optional metadata pair charge=%d error=%v", got, err)
	}
	// Count persisted UTF-8 bytes, not runes; the charge function is pure arithmetic.
	m.StateBasename = "雪🙂"
	got, err = sqliteMetaCharge(m)
	if err != nil || got != want+116-int64(len(f.authority))+7 {
		t.Fatal("metadata text charged by rune count")
	}
}

func TestSQLiteMetadataNonceWrapSemanticRevisionAndRollbackDurability(t *testing.T) {
	f := interopLocation(t)
	m := interopMeta(f)
	for i := range m.DurabilityNonce {
		m.DurabilityNonce[i] = 255
	}
	interopInitialize(t, f, m)
	for _, expected := range [][16]byte{{}, {15: 1}} {
		before := metaQARead(t, f)
		after := metaQANext(t, before)
		if after.DurabilityNonce != expected {
			t.Fatal("test next-nonce oracle differs")
		}
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		if err := sqliteUpdateMeta(tx, before, after); err != nil {
			t.Fatal(err)
		}
		interopCommit(t, tx)
		interopClose(t, c)
		if got := metaQARead(t, f); !reflect.DeepEqual(got, after) || got.Revision != "1" || got.LogicalBytes != m.LogicalBytes {
			t.Fatal("nonce-only wrap spent a revision or logical charge")
		}
	}
	before := metaQARead(t, f)
	after := metaQANext(t, before)
	after.Revision = "2"
	after.SyncEnabled = true
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	if err := sqliteUpdateMeta(tx, before, after); err != nil {
		t.Fatal(err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	if got := metaQARead(t, f); !reflect.DeepEqual(got, before) {
		t.Fatal("staged metadata survived caller rollback")
	}
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	if err := sqliteUpdateMeta(tx, before, after); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	if got := metaQARead(t, f); !reflect.DeepEqual(got, after) {
		t.Fatal("next semantic revision not durable exactly once")
	}
}

func TestSQLiteMetadataCompareAndUpdateRejectsEachStaleFence(t *testing.T) {
	for _, which := range []string{"revision", "nonce", "charge"} {
		t.Run(which, func(t *testing.T) {
			f := interopLocation(t)
			initial := interopMeta(f)
			interopInitialize(t, f, initial)
			current := metaQANext(t, initial)
			current.Revision = "2"
			current.LogicalBytes++
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if err := sqliteUpdateMeta(tx, initial, current); err != nil {
				t.Fatal(err)
			}
			interopCommit(t, tx)
			interopClose(t, c)
			stale := current
			switch which {
			case "revision":
				stale.Revision = initial.Revision
			case "nonce":
				stale.DurabilityNonce = initial.DurabilityNonce
			case "charge":
				stale.LogicalBytes = initial.LogicalBytes
			}
			after := metaQANext(t, stale) // Locally valid next-nonce transition, stale WHERE fence.
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			err := sqliteUpdateMeta(tx, stale, after)
			interopSafeError(t, err, f.directory)
			got, readErr := sqliteReadMeta(tx, f.authority, f.database)
			if readErr != nil || !reflect.DeepEqual(got, current) {
				t.Fatal("stale metadata substituted a write or altered current row", readErr)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			if got := metaQARead(t, f); !reflect.DeepEqual(got, current) {
				t.Fatal("stale mutation became durable")
			}
		})
	}
}

func TestSQLiteMetadataRefusesInvalidTransitionsAndPublicOverflow(t *testing.T) {
	for _, which := range []string{"revision_jump", "same_revision_sync", "same_revision_charge", "nonce", "computer", "authority", "database", "migration", "max_revision"} {
		t.Run(which, func(t *testing.T) {
			f := interopLocation(t)
			before := interopMeta(f)
			if which == "max_revision" {
				before.Revision = "18446744073709551615"
			}
			interopInitialize(t, f, before)
			after := metaQANext(t, before)
			switch which {
			case "revision_jump":
				after.Revision = "3"
			case "same_revision_sync":
				after.SyncEnabled = true
			case "same_revision_charge":
				after.LogicalBytes++
			case "nonce":
				after.DurabilityNonce = before.DurabilityNonce
			case "computer":
				after.ComputerID = "00000000-0000-4000-8000-000000000099"
			case "authority":
				after.StateBasename = "different.json"
			case "database":
				after.DatabaseBasename = "different.sqlite3"
			case "migration":
				after.Revision = "2"
				id := "00000000-0000-4000-8000-000000000004"
				sum := strings.Repeat("b", 64)
				after.MigrationID = &id
				after.BackupSHA256 = &sum
				after.LogicalBytes += 116
			case "max_revision":
				after.Revision = "1"
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			err := sqliteUpdateMeta(tx, before, after)
			interopSafeError(t, err, f.directory, after.ComputerID, after.StateBasename, after.DatabaseBasename)
			got, err := sqliteReadMeta(tx, f.authority, f.database)
			if err != nil || !reflect.DeepEqual(got, before) {
				t.Fatal("invalid transition changed row", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			if got := metaQARead(t, f); !reflect.DeepEqual(got, before) {
				t.Fatal("invalid transition became durable")
			}
		})
	}
}

func TestSQLiteMetadataLogicalFloorCeilingAndOverLimitDiagnosticRead(t *testing.T) {
	for _, which := range []string{"floor", "migration_pair", "below_floor", "negative", "ceiling", "over_ceiling"} {
		t.Run(which, func(t *testing.T) {
			f := interopLocation(t)
			meta := interopMeta(f)
			switch which {
			case "migration_pair":
				id := "00000000-0000-4000-8000-000000000004"
				sum := strings.Repeat("b", 64)
				meta.MigrationID = &id
				meta.BackupSHA256 = &sum
				meta.LogicalBytes += 116
			case "below_floor":
				meta.LogicalBytes--
			case "negative":
				meta.LogicalBytes = -1
			case "ceiling":
				meta.LogicalBytes = 64 << 20
			case "over_ceiling":
				meta.LogicalBytes = (64 << 20) + 1
			}
			c, tx := interopOpen(t, f, true, sqliteio.Write)
			if err := sqliteCreateSchema(tx); err != nil {
				t.Fatal(err)
			}
			err := sqliteInsertMeta(tx, meta)
			if which == "floor" || which == "migration_pair" || which == "ceiling" {
				if err != nil {
					t.Fatal(err)
				}
				interopCommit(t, tx)
				interopClose(t, c)
				if got := metaQARead(t, f); !reflect.DeepEqual(got, meta) {
					t.Fatal("accepted exact threshold changed")
				}
			} else {
				interopSafeError(t, err, f.directory)
				if which == "over_ceiling" {
					metaQACapacity(t, err)
				}
				if interopCount(t, tx, "SELECT count(*) FROM store_meta") != 0 {
					t.Fatal("refused metadata inserted a row")
				}
				interopRollback(t, tx)
				interopClose(t, c)
			}
		})
	}
	f := interopLocation(t)
	before := interopMeta(f)
	interopInitialize(t, f, before)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	interopDone(t, tx, "UPDATE store_meta SET logical_bytes=? WHERE singleton=1", sqliteio.Integer(math.MaxInt64))
	interopCommit(t, tx)
	interopClose(t, c)
	got := metaQARead(t, f)
	if got.LogicalBytes != math.MaxInt64 {
		t.Fatal("over-limit diagnostic read normalized counter")
	}
	after := metaQANext(t, got)
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	err := sqliteUpdateMeta(tx, got, after)
	metaQACapacity(t, err)
	interopRollback(t, tx)
	interopClose(t, c)
	if now := metaQARead(t, f); !reflect.DeepEqual(now, got) {
		t.Fatal("refused over-limit write changed diagnostic state")
	}
	// Existing over-limit state blocks an ordinary semantic write even when its
	// proposed counter is within capacity. All other transition inputs are valid.
	after = metaQANext(t, got)
	after.Revision = "2"
	after.LogicalBytes = 64 << 20
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	err = sqliteUpdateMeta(tx, got, after)
	var capacity *Error
	if !errors.As(err, &capacity) || capacity.Code != "validation" || capacity.Details["reason"] != "logical_capacity" {
		t.Errorf("ordinary update of existing over-limit state was not refused for logical capacity: %v", err)
	}
	if err != nil {
		interopSafeError(t, err, f.directory)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	if now := metaQARead(t, f); !reflect.DeepEqual(now, got) {
		t.Error("over-limit semantic attempt changed durable revision, nonce or counter after rollback")
	}
	// An otherwise valid semantic transition cannot newly exceed the ceiling.
	f = interopLocation(t)
	before = interopMeta(f)
	before.LogicalBytes = 64 << 20
	interopInitialize(t, f, before)
	after = metaQANext(t, before)
	after.Revision = "2"
	after.LogicalBytes++
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	err = sqliteUpdateMeta(tx, before, after)
	metaQACapacity(t, err)
	interopRollback(t, tx)
	interopClose(t, c)
	if now := metaQARead(t, f); !reflect.DeepEqual(now, before) {
		t.Fatal("one-byte-over update became durable")
	}
}

func TestSQLiteMetadataTypedReadRejectsIdentityAndShapeDisagreement(t *testing.T) {
	for _, which := range []string{"missing", "computer", "authority", "database", "migration_uuid", "backup_sha", "expected_authority", "expected_database"} {
		t.Run(which, func(t *testing.T) {
			f := interopLocation(t)
			m := interopMeta(f)
			interopInitialize(t, f, m)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if which == "missing" {
				interopDone(t, tx, "DELETE FROM store_meta")
			} else if which == "computer" {
				interopDone(t, tx, "UPDATE store_meta SET computer_id='invalid-computer-marker'")
			} else if which == "authority" {
				interopDone(t, tx, "UPDATE store_meta SET state_basename=?,logical_bytes=logical_bytes+?", sqliteio.Text("foreign-authority-marker"), sqliteio.Integer(int64(len("foreign-authority-marker")-len(f.authority))))
			} else if which == "database" {
				interopDone(t, tx, "UPDATE store_meta SET database_basename='foreign-database-marker'")
			} else if which == "migration_uuid" {
				interopDone(t, tx, "UPDATE store_meta SET logical_bytes=logical_bytes+116,migration_id='invalid-migration-marker',backup_sha256=?", sqliteio.Text(strings.Repeat("c", 64)))
			} else if which == "backup_sha" {
				interopDone(t, tx, "UPDATE store_meta SET logical_bytes=logical_bytes+116,migration_id='00000000-0000-4000-8000-000000000004',backup_sha256='invalid-backup-marker'")
			}
			a, b := f.authority, f.database
			if which == "expected_authority" {
				a = "wrong-expected-marker"
			}
			if which == "expected_database" {
				b = "wrong-expected-marker"
			}
			_, err := sqliteReadMeta(tx, a, b)
			interopSafeError(t, err, f.directory, "invalid-computer-marker", "foreign-authority-marker", "foreign-database-marker", "invalid-migration-marker", "invalid-backup-marker", "wrong-expected-marker")
			interopRollback(t, tx)
			interopClose(t, c)
			if got := metaQARead(t, f); !reflect.DeepEqual(got, m) {
				t.Fatal("typed refusal or rollback altered valid metadata")
			}
		})
	}
	// Relaxed structural fixture isolates decoder checks which STRICT/CHECK would
	// otherwise reject before read. This is not a claim production DDL admits them.
	for _, which := range []string{"version", "legacy_version", "revision_kind", "revision_width", "zero_revision", "sync_flag", "nonce_kind", "nonce_width", "pair", "extra", "charge_floor"} {
		t.Run(which, func(t *testing.T) {
			f := interopLocation(t)
			m := interopMeta(f)
			c, tx := interopOpen(t, f, true, sqliteio.Write)
			interopDone(t, tx, "CREATE TABLE store_meta(singleton,schema_version,legacy_schema_version,computer_id,revision,sync_enabled,durability_nonce,state_basename,database_basename,migration_id,backup_sha256,logical_bytes)")
			interopDone(t, tx, "INSERT INTO store_meta VALUES(1,1,1,?,?,0,?,?,?,NULL,NULL,?)", sqliteio.Text(interopComputer), interopCounter(t, "1"), sqliteio.Blob(m.DurabilityNonce[:]), sqliteio.Text(f.authority), sqliteio.Text(f.database), sqliteio.Integer(m.LogicalBytes))
			baseline, err := sqliteReadMeta(tx, f.authority, f.database)
			if err != nil || !reflect.DeepEqual(baseline, m) {
				t.Fatalf("relaxed decoder fixture baseline invalid: %v", err)
			}
			switch which {
			case "version":
				interopDone(t, tx, "UPDATE store_meta SET schema_version=2")
			case "legacy_version":
				interopDone(t, tx, "UPDATE store_meta SET legacy_schema_version=2")
			case "revision_kind":
				interopDone(t, tx, "UPDATE store_meta SET revision='00000001'")
			case "revision_width":
				interopDone(t, tx, "UPDATE store_meta SET revision=X'01'")
			case "zero_revision":
				interopDone(t, tx, "UPDATE store_meta SET revision=zeroblob(8)")
			case "sync_flag":
				interopDone(t, tx, "UPDATE store_meta SET sync_enabled=2")
			case "nonce_kind":
				interopDone(t, tx, "UPDATE store_meta SET durability_nonce='0000000000000000'")
			case "nonce_width":
				interopDone(t, tx, "UPDATE store_meta SET durability_nonce=zeroblob(15)")
			case "pair":
				interopDone(t, tx, "UPDATE store_meta SET logical_bytes=logical_bytes+44,migration_id='00000000-0000-4000-8000-000000000004'")
			case "extra":
				interopDone(t, tx, "INSERT INTO store_meta SELECT * FROM store_meta")
			case "charge_floor":
				interopDone(t, tx, "UPDATE store_meta SET logical_bytes=1")
			}
			_, err = sqliteReadMeta(tx, f.authority, f.database)
			interopSafeError(t, err, f.directory, "store_meta", "00000001")
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}
