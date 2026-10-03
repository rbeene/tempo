//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "github.com/rbeene/tempo/internal/activity/sqliteio"

func sqliteIntervalComponentCharge(intervalID, componentID string) (int64, error) {
	if !validUUID(intervalID) || !sqliteValidComponentID(componentID) {
		return 0, failure("validation")
	}
	return sqliteRowCharge(0, 0, []string{intervalID, componentID}, nil)
}

func sqliteReadIntervalComponent(tx *sqliteio.Tx, computer, intervalID string) (string, bool, error) {
	if !validUUID(computer) || !validUUID(intervalID) {
		return "", false, failure("validation")
	}
	s, err := tx.Prepare("SELECT interval_id,component_id FROM interval_components WHERE interval_id=? ORDER BY component_id", sqliteio.Text(intervalID))
	if err != nil {
		return "", false, err
	}
	component, found, err := sqliteReadIntervalComponentStatement(s, intervalID)
	if err != nil || !found {
		return component, found, err
	}
	// The complete selected scalar is closed before resolving its local owner.
	_, ownerFound, err := sqliteReadIntervalLocal(tx, computer, intervalID)
	if err != nil {
		return "", false, err
	}
	if !ownerFound {
		return "", false, failure("state_corrupt")
	}
	return component, true, nil
}

func sqliteReadIntervalComponentStatement(s *sqliteio.Stmt, intervalID string) (result string, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = "", false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return "", false, err
	}
	if s.ColumnCount() != 2 {
		return "", false, failure("state_corrupt")
	}
	owner, err := sqliteHostNormalizationText(s, 0)
	if err != nil {
		return "", false, err
	}
	component, err := sqliteHostNormalizationText(s, 1)
	if err != nil {
		return "", false, err
	}
	if owner != intervalID || !sqliteValidComponentID(component) {
		return "", false, failure("state_corrupt")
	}
	// The composite PK alone permits multiple seals; fresh intervals retain one.
	if present, err = s.Step(); err != nil {
		return "", false, err
	} else if present {
		return "", false, failure("state_corrupt")
	}
	return component, true, nil
}

func sqliteInsertIntervalComponent(tx *sqliteio.Tx, computer, intervalID, componentID string) (delta int64, err error) {
	if !validUUID(computer) {
		return 0, failure("validation")
	}
	charge, err := sqliteIntervalComponentCharge(intervalID, componentID)
	if err != nil {
		return 0, err
	}
	_, found, err := sqliteReadIntervalLocal(tx, computer, intervalID)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, failure("state_corrupt")
	}
	_, found, err = sqliteReadIntervalComponent(tx, computer, intervalID)
	if err != nil {
		return 0, err
	}
	if found {
		return 0, failure("validation")
	}
	s, err := tx.Prepare("INSERT INTO interval_components(interval_id,component_id) VALUES(?,?) RETURNING interval_id,component_id", sqliteio.Text(intervalID), sqliteio.Text(componentID))
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteHostNormalizationReturnedKeys(s, intervalID, componentID); err != nil {
		return 0, err
	}
	return charge, nil
}
