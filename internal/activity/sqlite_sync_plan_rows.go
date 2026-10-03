//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"math"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// This scalar is not a SyncPlan with invented or missing Parts.
type sqliteSyncPlanLocalRow struct {
	IntervalID, CompanySource string
	Configuration             SyncConfiguration
}

const sqliteSyncPlanLocalColumns = "interval_id,company_source,config_account_id,config_user_id,config_revision,config_mode,config_duration_policy,config_policy_version,config_clock,config_declared,config_declared_at_sec,config_declared_at_nsec,config_declared_at_json,config_source"
const sqliteSyncPlanLocalOld = "interval_id=? AND company_source=? AND config_account_id=? AND config_user_id=? AND config_revision=? AND config_mode=? AND config_duration_policy=? AND config_policy_version=? AND config_clock IS ? AND config_declared=? AND config_declared_at_sec=? AND config_declared_at_nsec=? AND config_declared_at_json=? AND config_source=?"

func sqliteEncodeSyncPlanLocal(row sqliteSyncPlanLocalRow) ([14]sqliteio.Value, int64, error) {
	var values [14]sqliteio.Value
	if !validUUID(row.IntervalID) || (row.CompanySource != "company_verified" && row.CompanySource != "user_declared_fallback") {
		return values, 0, failure("validation")
	}
	config, err := sqliteEncodeSyncConfiguration(row.Configuration)
	if err != nil {
		return values, 0, err
	}
	values[0], values[1] = sqliteio.Text(row.IntervalID), sqliteio.Text(row.CompanySource)
	copy(values[2:], config.values[1:])
	texts := make([]string, 0, len(config.texts)+1)
	texts = append(texts, row.IntervalID, row.CompanySource)
	texts = append(texts, config.texts[1:]...)
	charge, err := sqliteRowCharge(3, config.nulls, texts, [][]byte{config.revision[:]})
	if err != nil {
		return [14]sqliteio.Value{}, 0, err
	}
	return values, charge, nil
}

func sqliteSyncPlanLocalCharge(row sqliteSyncPlanLocalRow) (int64, error) {
	_, charge, err := sqliteEncodeSyncPlanLocal(row)
	return charge, err
}

func sqliteDecodeSyncPlanLocal(s *sqliteio.Stmt) (sqliteSyncPlanLocalRow, error) {
	var row sqliteSyncPlanLocalRow
	if s.ColumnCount() != 14 {
		return row, failure("state_corrupt")
	}
	id, err := sqliteDependencyText(s, 0)
	if err != nil {
		return row, err
	}
	source, err := sqliteDependencyText(s, 1)
	if err != nil {
		return row, err
	}
	config, err := sqliteDecodeSyncConfigurationFields(s, 2)
	if err != nil {
		return row, err
	}
	if !validUUID(id) || (source != "company_verified" && source != "user_declared_fallback") {
		return row, failure("state_corrupt")
	}
	return sqliteSyncPlanLocalRow{IntervalID: id, CompanySource: source, Configuration: config}, nil
}

func sqliteReadSyncPlanLocalStatement(s *sqliteio.Stmt, intervalID string) (result sqliteSyncPlanLocalRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = sqliteSyncPlanLocalRow{}
			found = false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	result, err = sqliteDecodeSyncPlanLocal(s)
	if err != nil {
		return result, false, err
	}
	if result.IntervalID != intervalID {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteReadSyncPlanLocal(tx *sqliteio.Tx, computer, intervalID string) (sqliteSyncPlanLocalRow, bool, error) {
	if !validUUID(computer) || !validUUID(intervalID) {
		return sqliteSyncPlanLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteSyncPlanLocalColumns+" FROM sync_plans WHERE interval_id=?", sqliteio.Text(intervalID))
	if err != nil {
		return sqliteSyncPlanLocalRow{}, false, err
	}
	row, found, err := sqliteReadSyncPlanLocalStatement(s, intervalID)
	if err != nil || !found {
		return row, found, err
	}
	// The primary cursor is closed before resolving the actual scoped parent.
	// Frozen consent need not match mutable current consent, and PlanPresent may
	// still be false while the caller stages a complete graph.
	_, parent, err := sqliteReadOutboxLocal(tx, computer, intervalID)
	if err != nil {
		return sqliteSyncPlanLocalRow{}, false, err
	}
	if !parent {
		return sqliteSyncPlanLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

// These two fixed prefix probes prove header replacement cannot erase consent
// beneath children, including attempts staged with deferred parent FKs.
func sqliteSyncPlanEmptyChildren(s *sqliteio.Stmt, intervalID string, attempts bool) (err error) {
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	present, err := s.Step()
	if err != nil || !present {
		return err
	}
	columns := 2
	if attempts {
		columns = 3
	}
	if s.ColumnCount() != columns {
		return failure("state_corrupt")
	}
	owner, err := sqliteDependencyText(s, 0)
	if err != nil {
		return err
	}
	if owner != intervalID {
		return failure("state_corrupt")
	}
	part, err := sqliteLocalRangeInteger(s, 1)
	if err != nil {
		return err
	}
	if part < 0 || part >= 100 {
		return failure("state_corrupt")
	}
	if attempts {
		ordinal, readErr := sqliteLocalRangeInteger(s, 2)
		if readErr != nil {
			return readErr
		}
		if ordinal < 0 || ordinal == math.MaxInt64 {
			return failure("state_corrupt")
		}
	}
	if present, err = s.Step(); err != nil {
		return err
	} else if present {
		return failure("state_corrupt")
	}
	return failure("state_corrupt")
}

func sqliteWriteSyncPlanLocal(tx *sqliteio.Tx, computer string, before *sqliteSyncPlanLocalRow, after sqliteSyncPlanLocalRow) (delta int64, err error) {
	if !validUUID(computer) || before != nil && before.IntervalID != after.IntervalID {
		return 0, failure("validation")
	}
	next, nextCharge, err := sqliteEncodeSyncPlanLocal(after)
	if err != nil {
		return 0, err
	}
	var old [14]sqliteio.Value
	var oldCharge int64
	if before != nil {
		old, oldCharge, err = sqliteEncodeSyncPlanLocal(*before)
		if err != nil {
			return 0, err
		}
	}
	_, parent, err := sqliteReadOutboxLocal(tx, computer, after.IntervalID)
	if err != nil {
		return 0, err
	}
	if !parent {
		return 0, failure("state_corrupt")
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("INSERT INTO sync_plans("+sqliteSyncPlanLocalColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING interval_id", next[:]...)
	} else {
		s, err = tx.Prepare("SELECT interval_id,ordinal FROM sync_parts WHERE interval_id=? LIMIT 1", sqliteio.Text(after.IntervalID))
		if err != nil {
			return 0, err
		}
		if err = sqliteSyncPlanEmptyChildren(s, after.IntervalID, false); err != nil {
			return 0, err
		}
		s, err = tx.Prepare("SELECT interval_id,part_ordinal,ordinal FROM sync_attempts WHERE interval_id=? LIMIT 1", sqliteio.Text(after.IntervalID))
		if err != nil {
			return 0, err
		}
		if err = sqliteSyncPlanEmptyChildren(s, after.IntervalID, true); err != nil {
			return 0, err
		}
		values := make([]sqliteio.Value, 0, 27)
		values = append(values, next[1:]...)
		values = append(values, old[:]...)
		s, err = tx.Prepare("UPDATE sync_plans SET company_source=?,config_account_id=?,config_user_id=?,config_revision=?,config_mode=?,config_duration_policy=?,config_policy_version=?,config_clock=?,config_declared=?,config_declared_at_sec=?,config_declared_at_nsec=?,config_declared_at_json=?,config_source=? WHERE "+sqliteSyncPlanLocalOld+" RETURNING interval_id", values...)
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
	if err = sqliteLocalReturnedID(s, after.IntervalID); err != nil {
		return 0, err
	}
	return nextCharge - oldCharge, nil
}
