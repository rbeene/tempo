//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/identity"
)

const sqliteSyncConfigurationColumns = "config_key,account_id,user_id,revision,mode,duration_policy,policy_version,clock,declared,declared_at_sec,declared_at_nsec,declared_at_json,source"
const sqliteSyncConfigurationOld = "config_key=? AND account_id=? AND user_id=? AND revision=? AND mode=? AND duration_policy=? AND policy_version=? AND clock IS ? AND declared=? AND declared_at_sec=? AND declared_at_nsec=? AND declared_at_json=? AND source=?"

// A frozen plan embeds these exact twelve configuration fields. The key is the
// configuration table's additional column, never a second copy in a plan.
type sqliteEncodedSyncConfiguration struct {
	values   [13]sqliteio.Value
	key      string
	texts    []string
	revision [8]byte
	nulls    int
	charge   int64
}

func sqliteValidSyncConfiguration(row SyncConfiguration) bool {
	for _, value := range []string{row.AccountID, row.UserID, row.Revision, row.Mode, row.DurationPolicy, row.PolicyVersion, row.Source} {
		if !utf8.ValidString(value) {
			return false
		}
	}
	return (row.Clock == nil || utf8.ValidString(*row.Clock)) && validSyncConfiguration(row)
}

func sqliteEncodeSyncConfiguration(row SyncConfiguration) (sqliteEncodedSyncConfiguration, error) {
	if !sqliteValidSyncConfiguration(row) {
		return sqliteEncodedSyncConfiguration{}, failure("validation")
	}
	revision, err := sqliteEncodeUint64(row.Revision)
	if err != nil {
		return sqliteEncodedSyncConfiguration{}, failure("validation")
	}
	declared, err := sqliteEncodeTime(row.DeclaredAt)
	if err != nil {
		return sqliteEncodedSyncConfiguration{}, failure("validation")
	}
	materialized := row
	materialized.Clock = sqliteCopyString(row.Clock)
	materialized.DeclaredAt, err = sqliteDecodeTime(declared)
	if err != nil || !sqliteValidSyncConfiguration(materialized) {
		return sqliteEncodedSyncConfiguration{}, failure("validation")
	}
	key := syncConfigKey(materialized.AccountID, materialized.UserID)
	encoded := sqliteEncodedSyncConfiguration{key: key, revision: revision, values: [13]sqliteio.Value{
		sqliteio.Text(key), sqliteio.Text(materialized.AccountID), sqliteio.Text(materialized.UserID), sqliteio.Blob(revision[:]),
		sqliteio.Text(materialized.Mode), sqliteio.Text(materialized.DurationPolicy), sqliteio.Text(materialized.PolicyVersion), sqliteMetaOptional(materialized.Clock),
		sqliteMetaBool(materialized.Declared), sqliteio.Integer(declared.Seconds), sqliteio.Integer(declared.Nanoseconds), sqliteio.Text(declared.JSON), sqliteio.Text(materialized.Source),
	}}
	encoded.texts = []string{key, materialized.AccountID, materialized.UserID, materialized.Mode, materialized.DurationPolicy, materialized.PolicyVersion, declared.JSON, materialized.Source}
	if materialized.Clock == nil {
		encoded.nulls = 1
	} else {
		encoded.texts = append(encoded.texts, *materialized.Clock)
	}
	encoded.charge, err = sqliteRowCharge(3, encoded.nulls, encoded.texts, [][]byte{revision[:]})
	if err != nil {
		return sqliteEncodedSyncConfiguration{}, err
	}
	return encoded, nil
}

func sqliteSyncConfigurationCharge(row SyncConfiguration) (int64, error) {
	encoded, err := sqliteEncodeSyncConfiguration(row)
	return encoded.charge, err
}

// Only configuration13 and plan14 call this fixed twelve-column decoder.
// Their callers check full projection size and the owning primary key.
func sqliteDecodeSyncConfigurationFields(s *sqliteio.Stmt, start int) (SyncConfiguration, error) {
	var row SyncConfiguration
	for _, field := range []struct {
		offset int
		target *string
	}{
		{0, &row.AccountID}, {1, &row.UserID}, {3, &row.Mode}, {4, &row.DurationPolicy}, {5, &row.PolicyVersion}, {11, &row.Source},
	} {
		value, err := sqliteDependencyText(s, start+field.offset)
		if err != nil {
			return SyncConfiguration{}, err
		}
		*field.target = value
	}
	kind, err := s.Kind(start + 2)
	if err != nil {
		return SyncConfiguration{}, errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.BlobKind {
		return SyncConfiguration{}, failure("state_corrupt")
	}
	blob, err := s.Blob(start + 2)
	if err != nil {
		return SyncConfiguration{}, err
	}
	row.Revision, err = sqliteDecodeUint64(blob)
	if err != nil {
		return SyncConfiguration{}, err
	}
	kind, err = s.Kind(start + 6)
	if err != nil {
		return SyncConfiguration{}, errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.NullKind {
		value, readErr := sqliteDependencyText(s, start+6)
		if readErr != nil {
			return SyncConfiguration{}, readErr
		}
		row.Clock = &value
	}
	declared, err := sqliteLocalRangeInteger(s, start+7)
	if err != nil {
		return SyncConfiguration{}, err
	}
	if declared != 0 && declared != 1 {
		return SyncConfiguration{}, failure("state_corrupt")
	}
	row.Declared = declared == 1
	row.DeclaredAt, err = sqliteLocalRangeTime(s, start+8)
	if err != nil {
		return SyncConfiguration{}, err
	}
	if !sqliteValidSyncConfiguration(row) {
		return SyncConfiguration{}, failure("state_corrupt")
	}
	return row, nil
}

func sqliteDecodeSyncConfiguration(s *sqliteio.Stmt) (SyncConfiguration, string, error) {
	if s.ColumnCount() != 13 {
		return SyncConfiguration{}, "", failure("state_corrupt")
	}
	key, err := sqliteDependencyText(s, 0)
	if err != nil {
		return SyncConfiguration{}, "", err
	}
	row, err := sqliteDecodeSyncConfigurationFields(s, 1)
	if err != nil {
		return SyncConfiguration{}, "", err
	}
	if key != syncConfigKey(row.AccountID, row.UserID) {
		return SyncConfiguration{}, "", failure("state_corrupt")
	}
	return row, key, nil
}

func sqliteReadSyncConfiguration(tx *sqliteio.Tx, account, user string) (result SyncConfiguration, found bool, err error) {
	if !identity.Valid(account) || !identity.Valid(user) {
		return result, false, failure("validation")
	}
	key := syncConfigKey(account, user)
	s, err := tx.Prepare("SELECT "+sqliteSyncConfigurationColumns+" FROM sync_configurations WHERE config_key=?", sqliteio.Text(key))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = SyncConfiguration{}
			found = false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	var selected string
	result, selected, err = sqliteDecodeSyncConfiguration(s)
	if err != nil {
		return result, false, err
	}
	if selected != key || result.AccountID != account || result.UserID != user {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteSyncConfigurations(tx *sqliteio.Tx) (result []SyncConfiguration, err error) {
	s, err := tx.Prepare("SELECT " + sqliteSyncConfigurationColumns + " FROM sync_configurations ORDER BY config_key")
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([]SyncConfiguration, 0)
	previous := ""
	for {
		present, readErr := s.Step()
		if readErr != nil {
			return nil, readErr
		}
		if !present {
			return result, nil
		}
		row, key, readErr := sqliteDecodeSyncConfiguration(s)
		if readErr != nil {
			return nil, readErr
		}
		if len(result) > 0 && key <= previous {
			return nil, failure("state_corrupt")
		}
		previous = key
		result = append(result, row)
	}
}

func sqliteWriteSyncConfiguration(tx *sqliteio.Tx, before *SyncConfiguration, after SyncConfiguration) (delta int64, err error) {
	if before != nil && (before.AccountID != after.AccountID || before.UserID != after.UserID) {
		return 0, failure("validation")
	}
	next, err := sqliteEncodeSyncConfiguration(after)
	if err != nil {
		return 0, err
	}
	var old sqliteEncodedSyncConfiguration
	if before != nil {
		old, err = sqliteEncodeSyncConfiguration(*before)
		if err != nil {
			return 0, err
		}
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("INSERT INTO sync_configurations("+sqliteSyncConfigurationColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING config_key", next.values[:]...)
	} else {
		values := make([]sqliteio.Value, 0, 25)
		values = append(values, next.values[1:]...)
		values = append(values, old.values[:]...)
		s, err = tx.Prepare("UPDATE sync_configurations SET account_id=?,user_id=?,revision=?,mode=?,duration_policy=?,policy_version=?,clock=?,declared=?,declared_at_sec=?,declared_at_nsec=?,declared_at_json=?,source=? WHERE "+sqliteSyncConfigurationOld+" RETURNING config_key", values...)
	}
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedID(s, next.key); err != nil {
		return 0, err
	}
	return next.charge - old.charge, nil
}
