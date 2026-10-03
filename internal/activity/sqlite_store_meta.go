//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteStoreMeta struct {
	ComputerID, Revision            string
	StateBasename, DatabaseBasename string
	SyncEnabled                     bool
	DurabilityNonce                 [16]byte
	MigrationID, BackupSHA256       *string
	LogicalBytes                    int64
}

// Schema creation and the first domain mutation share the caller's transaction.
// An absent store's revision zero remains in memory until final revision one.
func sqliteCreateSchema(tx *sqliteio.Tx) error {
	s, err := tx.Prepare("SELECT 1 FROM sqlite_schema LIMIT 1")
	if err != nil {
		return err
	}
	row, err := s.Step()
	if err == nil && row {
		err = failure("state_corrupt")
	}
	if err = sqliteCloseMetaStatement(s, err); err != nil {
		return err
	}
	return tx.InstallSchema(sqliteSchema)
}

func sqliteInsertMeta(tx *sqliteio.Tx, meta sqliteStoreMeta) (err error) {
	if err = sqliteValidateMeta(meta); err != nil {
		return err
	}
	if err = sqliteMetaCapacity(meta); err != nil {
		return err
	}
	revision, err := sqliteEncodeUint64(meta.Revision)
	if err != nil {
		return err
	}
	s, err := tx.Prepare(`INSERT INTO store_meta
		(singleton,schema_version,legacy_schema_version,computer_id,revision,sync_enabled,
		durability_nonce,state_basename,database_basename,migration_id,backup_sha256,logical_bytes)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		sqliteio.Integer(1), sqliteio.Integer(sqliteSchemaVersion), sqliteio.Integer(stateVersion),
		sqliteio.Text(meta.ComputerID), sqliteio.Blob(revision[:]), sqliteMetaBool(meta.SyncEnabled),
		sqliteio.Blob(meta.DurabilityNonce[:]), sqliteio.Text(meta.StateBasename), sqliteio.Text(meta.DatabaseBasename),
		sqliteMetaOptional(meta.MigrationID), sqliteMetaOptional(meta.BackupSHA256), sqliteio.Integer(meta.LogicalBytes))
	if err != nil {
		return err
	}
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	row, err := s.Step()
	if err == nil && row {
		err = failure("state_corrupt")
	}
	return err
}

func sqliteReadMeta(tx *sqliteio.Tx, authorityBase, databaseBase string) (meta sqliteStoreMeta, err error) {
	s, err := tx.Prepare(`SELECT singleton,schema_version,legacy_schema_version,computer_id,revision,
		sync_enabled,durability_nonce,state_basename,database_basename,migration_id,backup_sha256,logical_bytes
		FROM store_meta`)
	if err != nil {
		return meta, err
	}
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	row, err := s.Step()
	if err != nil {
		return meta, err
	}
	if !row || s.ColumnCount() != 12 {
		return meta, failure("state_corrupt")
	}
	kinds := [12]sqliteio.Kind{sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.IntegerKind,
		sqliteio.TextKind, sqliteio.BlobKind, sqliteio.IntegerKind, sqliteio.BlobKind,
		sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.IntegerKind}
	for column, expected := range kinds {
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return meta, failure("state_corrupt")
		}
		if (column == 9 || column == 10) && kind == sqliteio.NullKind {
			continue
		}
		if kind != expected {
			return meta, failure("state_corrupt")
		}
		switch column {
		case 0, 1, 2:
			var value int64
			value, err = s.Int64(column)
			want := int64(1)
			if column == 1 {
				want = sqliteSchemaVersion
			} else if column == 2 {
				want = stateVersion
			}
			if err == nil && value != want {
				err = failure("state_corrupt")
			}
		case 3:
			meta.ComputerID, err = s.Text(column)
		case 4:
			var value []byte
			value, err = s.Blob(column)
			if err == nil {
				meta.Revision, err = sqliteDecodeUint64(value)
			}
		case 5:
			var value int64
			value, err = s.Int64(column)
			if err == nil && value != 0 && value != 1 {
				err = failure("state_corrupt")
			}
			meta.SyncEnabled = value == 1
		case 6:
			var value []byte
			value, err = s.Blob(column)
			if err == nil && len(value) != len(meta.DurabilityNonce) {
				err = failure("state_corrupt")
			}
			copy(meta.DurabilityNonce[:], value)
		case 7:
			meta.StateBasename, err = s.Text(column)
		case 8:
			meta.DatabaseBasename, err = s.Text(column)
		case 9, 10:
			var value string
			value, err = s.Text(column)
			if column == 9 {
				meta.MigrationID = &value
			} else {
				meta.BackupSHA256 = &value
			}
		case 11:
			meta.LogicalBytes, err = s.Int64(column)
		}
		if err != nil {
			return meta, err
		}
	}
	if row, err = s.Step(); err != nil {
		return meta, err
	} else if row {
		return meta, failure("state_corrupt")
	}
	if meta.StateBasename != authorityBase || meta.DatabaseBasename != databaseBase {
		return meta, failure("state_corrupt")
	}
	return meta, sqliteValidateMeta(meta)
}

func sqliteUpdateMeta(tx *sqliteio.Tx, before, after sqliteStoreMeta) (err error) {
	if err = sqliteValidateMeta(before); err != nil {
		return err
	}
	if err = sqliteValidateMeta(after); err != nil {
		return err
	}
	if err = sqliteMetaCapacity(before); err != nil {
		return err
	}
	if err = sqliteMetaCapacity(after); err != nil {
		return err
	}
	if before.ComputerID != after.ComputerID || before.StateBasename != after.StateBasename ||
		before.DatabaseBasename != after.DatabaseBasename || !sqliteMetaSameOptional(before.MigrationID, after.MigrationID) ||
		!sqliteMetaSameOptional(before.BackupSHA256, after.BackupSHA256) {
		return failure("validation")
	}
	nextNonce, err := sqliteNextNonce(before.DurabilityNonce[:])
	if err != nil {
		return err
	}
	if nextNonce != after.DurabilityNonce {
		return failure("validation")
	}
	oldRevision, _ := counter(before.Revision)
	newRevision, _ := counter(after.Revision)
	if newRevision == oldRevision {
		if before.SyncEnabled != after.SyncEnabled || before.LogicalBytes != after.LogicalBytes {
			return failure("validation")
		}
	} else if oldRevision == ^uint64(0) || newRevision != oldRevision+1 {
		return failure("validation")
	}
	oldBytes, err := sqliteEncodeUint64(before.Revision)
	if err != nil {
		return err
	}
	newBytes, err := sqliteEncodeUint64(after.Revision)
	if err != nil {
		return err
	}
	s, err := tx.Prepare(`UPDATE store_meta SET revision=?,sync_enabled=?,durability_nonce=?,logical_bytes=?
		WHERE singleton=1 AND revision=? AND durability_nonce=? AND logical_bytes=?
		AND schema_version=? AND legacy_schema_version=? AND computer_id=? AND state_basename=?
		AND database_basename=? AND migration_id IS ? AND backup_sha256 IS ? AND sync_enabled=?
		RETURNING singleton`,
		sqliteio.Blob(newBytes[:]), sqliteMetaBool(after.SyncEnabled), sqliteio.Blob(after.DurabilityNonce[:]), sqliteio.Integer(after.LogicalBytes),
		sqliteio.Blob(oldBytes[:]), sqliteio.Blob(before.DurabilityNonce[:]), sqliteio.Integer(before.LogicalBytes),
		sqliteio.Integer(sqliteSchemaVersion), sqliteio.Integer(stateVersion), sqliteio.Text(before.ComputerID),
		sqliteio.Text(before.StateBasename), sqliteio.Text(before.DatabaseBasename), sqliteMetaOptional(before.MigrationID),
		sqliteMetaOptional(before.BackupSHA256), sqliteMetaBool(before.SyncEnabled))
	if err != nil {
		return err
	}
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	row, err := s.Step()
	if err != nil {
		return err
	}
	if !row || s.ColumnCount() != 1 {
		return failure("state_corrupt")
	}
	singleton, err := s.Int64(0)
	if err != nil {
		return err
	}
	if singleton != 1 {
		return failure("state_corrupt")
	}
	if row, err = s.Step(); err == nil && row {
		err = failure("state_corrupt")
	}
	return err
}

func sqliteValidateMeta(meta sqliteStoreMeta) error {
	revision, ok := counter(meta.Revision)
	if !validUUID(meta.ComputerID) || !ok || revision == 0 || meta.StateBasename == "" ||
		meta.StateBasename == "." || meta.StateBasename == ".." || strings.ContainsAny(meta.StateBasename, "/\x00") {
		return failure("state_corrupt")
	}
	digest := sha256.Sum256([]byte("tempo-sqlite-v1\x00" + meta.StateBasename))
	if meta.DatabaseBasename != "activity-"+hex.EncodeToString(digest[:])+".sqlite3" ||
		(meta.MigrationID == nil) != (meta.BackupSHA256 == nil) {
		return failure("state_corrupt")
	}
	if meta.MigrationID != nil {
		if !validUUID(*meta.MigrationID) || len(*meta.BackupSHA256) != 64 {
			return failure("state_corrupt")
		}
		for _, c := range *meta.BackupSHA256 {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return failure("state_corrupt")
			}
		}
	}
	floor, err := sqliteMetaCharge(meta)
	if err != nil {
		return err
	}
	if meta.LogicalBytes < floor {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteMetaCapacity(meta sqliteStoreMeta) error {
	if meta.LogicalBytes > sqliteLogicalCapacity {
		return &Error{Code: "validation", Message: "local activity state capacity reached; preserve the existing state for review", Details: map[string]any{"reason": "logical_capacity"}}
	}
	return nil
}

func sqliteMetaBool(value bool) sqliteio.Value {
	if value {
		return sqliteio.Integer(1)
	}
	return sqliteio.Integer(0)
}

func sqliteMetaOptional(value *string) sqliteio.Value {
	if value == nil {
		return sqliteio.Null()
	}
	return sqliteio.Text(*value)
}

func sqliteMetaSameOptional(a, b *string) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func sqliteCloseMetaStatement(s *sqliteio.Stmt, cause error) error {
	if cleanup := s.Close(); cleanup != nil {
		if cause == nil {
			return cleanup
		}
		return errors.Join(cause, cleanup)
	}
	return cause
}
