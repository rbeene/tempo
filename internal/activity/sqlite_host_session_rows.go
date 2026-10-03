//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"path/filepath"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteHostSessionRow struct {
	Key   string
	Value hostSession
}

const sqliteHostSessionColumns = "source,native_session,session_key,incarnation,cwd,root_turn_key"

type sqliteEncodedHostSession struct {
	row    sqliteHostSessionRow
	values [6]sqliteio.Value
	charge int64
}

func sqliteReadHostSession(tx *sqliteio.Tx, computerID, source, nativeID string) (result sqliteHostSessionRow, found bool, err error) {
	if !validUUID(computerID) || !hostSource(source) || !safeIdentifier(nativeID, 256) {
		return result, false, failure("validation")
	}
	key := hostSessionKey(HostEvent{Source: source, SessionID: nativeID})
	s, err := tx.Prepare("SELECT "+sqliteHostSessionColumns+" FROM host_sessions WHERE session_key=?", sqliteio.Text(key))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteHostSessionRow{}, false
		}
	}()
	row, err := s.Step()
	if err != nil || !row {
		return result, false, err
	}
	if s.ColumnCount() != 6 {
		return result, false, failure("state_corrupt")
	}
	var texts [6]string
	for i := range texts {
		texts[i], err = sqliteHostNormalizationText(s, i)
		if err != nil {
			return result, false, err
		}
	}
	result = sqliteHostSessionRow{Key: texts[2], Value: hostSession{Source: texts[0], NativeID: texts[1], ID: texts[3], CWD: texts[4], RootTurn: texts[5]}}
	// Identity lookup is hashed on raw input; the selected projection is stored
	// text, unlike the raw direct comparisons used by historical-turn queries.
	if !sqliteValidHostSession(result) || result.Key != key || result.Value.Source != source || result.Value.NativeID != sqlitePersistClockString(nativeID) {
		return result, false, failure("state_corrupt")
	}
	if err = sqliteCheckHostSessionRoot(tx, computerID, result.Value); err != nil {
		return result, false, err
	}
	if row, err = s.Step(); err != nil {
		return result, false, err
	} else if row {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteValidHostSession(row sqliteHostSessionRow) bool {
	s := row.Value
	return validUUID(s.ID) && hostSource(s.Source) && safeIdentifier(s.NativeID, 256) && filepath.IsAbs(s.CWD) && row.Key == hostSessionKey(HostEvent{Source: s.Source, SessionID: s.NativeID})
}

func sqliteCheckHostSessionRoot(tx *sqliteio.Tx, computerID string, session hostSession) error {
	if session.RootTurn == "" {
		return nil
	}
	if len(session.RootTurn) != 64 {
		return failure("state_corrupt")
	}
	turn, found, err := sqliteReadHostTurn(tx, computerID, session.RootTurn)
	if err != nil {
		return err
	}
	if !found || turn.AgentID != "" || turn.Incarnation != session.ID || turn.Source != session.Source || turn.NativeSession != session.NativeID || turn.Actor == nil {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteEncodeHostSession(computerID string, row sqliteHostSessionRow) (sqliteEncodedHostSession, error) {
	if !validUUID(computerID) || !sqliteValidHostSession(row) {
		return sqliteEncodedHostSession{}, failure("validation")
	}
	s := row.Value
	texts := []string{s.Source, s.NativeID, row.Key, s.ID, s.CWD, s.RootTurn}
	for i := range texts {
		texts[i] = sqlitePersistClockString(texts[i])
	}
	encoded := sqliteEncodedHostSession{row: sqliteHostSessionRow{Key: texts[2], Value: hostSession{Source: texts[0], NativeID: texts[1], ID: texts[3], CWD: texts[4], RootTurn: texts[5]}}}
	if !sqliteValidHostSession(encoded.row) {
		return sqliteEncodedHostSession{}, failure("validation")
	}
	for i := range encoded.values {
		encoded.values[i] = sqliteio.Text(texts[i])
	}
	var err error
	encoded.charge, err = sqliteRowCharge(0, 0, texts, nil)
	if err != nil {
		return sqliteEncodedHostSession{}, err
	}
	return encoded, nil
}

func sqliteWriteHostSession(tx *sqliteio.Tx, computerID string, before *sqliteHostSessionRow, after sqliteHostSessionRow) (delta int64, err error) {
	if before != nil && (before.Key != after.Key || before.Value.Source != after.Value.Source || before.Value.NativeID != after.Value.NativeID) {
		return 0, failure("validation")
	}
	next, err := sqliteEncodeHostSession(computerID, after)
	if err != nil {
		return 0, err
	}
	var old sqliteEncodedHostSession
	if before != nil {
		old, err = sqliteEncodeHostSession(computerID, *before)
		if err != nil {
			return 0, err
		}
		if err = sqliteCheckHostSessionRoot(tx, computerID, old.row.Value); err != nil {
			return 0, err
		}
	}
	if err = sqliteCheckHostSessionRoot(tx, computerID, next.row.Value); err != nil {
		return 0, err
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("INSERT INTO host_sessions("+sqliteHostSessionColumns+") VALUES(?,?,?,?,?,?) RETURNING session_key", next.values[:]...)
	} else {
		values := []sqliteio.Value{next.values[3], next.values[4], next.values[5]}
		values = append(values, old.values[:]...)
		s, err = tx.Prepare(`UPDATE host_sessions SET incarnation=?,cwd=?,root_turn_key=?
			WHERE source=? AND native_session=? AND session_key=? AND incarnation=? AND cwd=? AND root_turn_key=? RETURNING session_key`, values...)
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
	if err = sqliteHostNormalizationReturnedKeys(s, next.row.Key); err != nil {
		return 0, err
	}
	return next.charge - old.charge, nil
}
