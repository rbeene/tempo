//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "github.com/rbeene/tempo/internal/activity/sqliteio"

type sqliteHostToolRow struct {
	TurnKey, ID string
	Value       hostTool
}

const sqliteHostToolColumns = "turn_key,tool_id,name,phase"

type sqliteEncodedHostTool struct {
	row    sqliteHostToolRow
	values [4]sqliteio.Value
	charge int64
}

func sqliteReadHostTool(tx *sqliteio.Tx, computerID, turnKey, toolID string) (result sqliteHostToolRow, found bool, err error) {
	if !validUUID(computerID) || len(turnKey) != 64 || !safeIdentifier(toolID, 256) {
		return result, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteHostToolColumns+" FROM host_tools WHERE turn_key=? AND tool_id=?", sqliteio.Text(turnKey), sqliteio.Text(toolID))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteHostToolRow{}, false
		}
	}()
	row, err := s.Step()
	if err != nil || !row {
		return result, false, err
	}
	result, err = sqliteDecodeHostTool(s)
	if err != nil {
		return result, false, err
	}
	if result.TurnKey != turnKey || result.ID != toolID {
		return result, false, failure("state_corrupt")
	}
	parent, present, err := sqliteReadHostTurn(tx, computerID, result.TurnKey)
	if err != nil {
		return result, false, err
	}
	if !present || !sqliteHostToolSourceAllowed(result, parent.Source) {
		return result, false, failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return result, false, err
	} else if row {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqlitePendingHostTools(tx *sqliteio.Tx, computerID, turnKey string) (result map[string]hostTool, err error) {
	if !validUUID(computerID) || len(turnKey) != 64 {
		return nil, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteHostToolColumns+" FROM host_tools WHERE turn_key=? AND phase='pre'", sqliteio.Text(turnKey))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = map[string]hostTool{}
	var parentSource string
	for {
		present, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !present {
			return result, nil
		}
		row, readErr := sqliteDecodeHostTool(s)
		if readErr != nil {
			return nil, readErr
		}
		if row.TurnKey != turnKey || row.Value.Phase != "pre" {
			return nil, failure("state_corrupt")
		}
		if parentSource == "" {
			parent, found, readErr := sqliteReadHostTurn(tx, computerID, turnKey)
			if readErr != nil {
				return nil, readErr
			}
			if !found {
				return nil, failure("state_corrupt")
			}
			parentSource = parent.Source
		}
		if !sqliteHostToolSourceAllowed(row, parentSource) {
			return nil, failure("state_corrupt")
		}
		if _, duplicate := result[row.ID]; duplicate {
			return nil, failure("state_corrupt")
		}
		result[row.ID] = row.Value
	}
}

func sqliteDecodeHostTool(s *sqliteio.Stmt) (sqliteHostToolRow, error) {
	if s.ColumnCount() != 4 {
		return sqliteHostToolRow{}, failure("state_corrupt")
	}
	var texts [4]string
	for i := range texts {
		value, err := sqliteHostNormalizationText(s, i)
		if err != nil {
			return sqliteHostToolRow{}, err
		}
		texts[i] = value
	}
	row := sqliteHostToolRow{TurnKey: texts[0], ID: texts[1], Value: hostTool{Name: texts[2], Phase: texts[3]}}
	if !sqliteValidHostTool(row) {
		return sqliteHostToolRow{}, failure("state_corrupt")
	}
	return row, nil
}

func sqliteValidHostTool(row sqliteHostToolRow) bool {
	return len(row.TurnKey) == 64 && safeIdentifier(row.ID, 256) && safeIdentifier(row.Value.Name, 256) &&
		(row.Value.Phase == "pre" || row.Value.Phase == "post" || row.Value.Phase == "failed")
}

func sqliteHostToolSourceAllowed(row sqliteHostToolRow, source string) bool {
	return hostSource(source) && (row.Value.Phase != "failed" || source == "claude")
}

func sqliteEncodeHostTool(computerID string, row sqliteHostToolRow) (sqliteEncodedHostTool, error) {
	if !validUUID(computerID) || !sqliteValidHostTool(row) {
		return sqliteEncodedHostTool{}, failure("validation")
	}
	texts := []string{row.TurnKey, row.ID, row.Value.Name, row.Value.Phase}
	for i := range texts {
		texts[i] = sqlitePersistClockString(texts[i])
	}
	// Selected new boundary: raw-valid tool text may expand during persistence.
	// Refuse that proposal before mutation without changing its lookup identity.
	if len(texts[1]) > 256 || len(texts[2]) > 256 {
		return sqliteEncodedHostTool{}, failure("validation")
	}
	encoded := sqliteEncodedHostTool{row: sqliteHostToolRow{TurnKey: texts[0], ID: texts[1], Value: hostTool{Name: texts[2], Phase: texts[3]}}}
	for i := range encoded.values {
		encoded.values[i] = sqliteio.Text(texts[i])
	}
	var err error
	encoded.charge, err = sqliteRowCharge(0, 0, texts, nil)
	if err != nil {
		return sqliteEncodedHostTool{}, err
	}
	return encoded, nil
}

func sqliteWriteHostTool(tx *sqliteio.Tx, computerID string, before *sqliteHostToolRow, after sqliteHostToolRow) (delta int64, err error) {
	if before != nil && (before.TurnKey != after.TurnKey || before.ID != after.ID) {
		return 0, failure("validation")
	}
	next, err := sqliteEncodeHostTool(computerID, after)
	if err != nil {
		return 0, err
	}
	var old sqliteEncodedHostTool
	if before != nil {
		old, err = sqliteEncodeHostTool(computerID, *before)
		if err != nil {
			return 0, err
		}
	}
	parent, found, err := sqliteReadHostTurn(tx, computerID, after.TurnKey)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, failure("state_corrupt")
	}
	if !sqliteHostToolSourceAllowed(after, parent.Source) || before != nil && !sqliteHostToolSourceAllowed(*before, parent.Source) {
		return 0, failure("validation")
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("INSERT INTO host_tools("+sqliteHostToolColumns+") VALUES(?,?,?,?) RETURNING turn_key,tool_id", next.values[:]...)
	} else {
		values := []sqliteio.Value{next.values[2], next.values[3]}
		values = append(values, old.values[:]...)
		s, err = tx.Prepare("UPDATE host_tools SET name=?,phase=? WHERE turn_key=? AND tool_id=? AND name=? AND phase=? RETURNING turn_key,tool_id", values...)
	}
	if err != nil {
		return 0, sqliteHostNormalizationWriteError(err)
	}
	defer func() {
		err = sqliteHostNormalizationWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteHostNormalizationReturnedKeys(s, next.row.TurnKey, next.row.ID); err != nil {
		return 0, err
	}
	return next.charge - old.charge, nil
}
