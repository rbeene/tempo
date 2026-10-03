//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"path/filepath"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteHostTurnRow struct {
	Key, Source, NativeSession, Incarnation, TurnID, AgentID, CWD string
	Actor                                                         *ActorRef
	Stopped                                                       bool
}

const sqliteHostTurnColumns = "turn_key,source,native_session,incarnation,turn_id,agent_id,cwd,actor_key,actor_generation,stopped"

type sqliteEncodedHostTurn struct {
	key    string
	values [10]sqliteio.Value
	charge int64
}

func sqliteReadHostTurn(tx *sqliteio.Tx, computerID, key string) (result sqliteHostTurnRow, found bool, err error) {
	if !validUUID(computerID) || len(key) != 64 {
		return result, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteHostTurnColumns+" FROM host_turns WHERE turn_key=?", sqliteio.Text(key))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteHostTurnRow{}, false
		}
	}()
	row, err := s.Step()
	if err != nil || !row {
		return result, false, err
	}
	result, err = sqliteDecodeHostTurn(tx, s, computerID)
	if err != nil {
		return result, false, err
	}
	if result.Key != key {
		return result, false, failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return result, false, err
	} else if row {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

// Decode one current row without loading any completed tool history or head.
func sqliteDecodeHostTurn(tx *sqliteio.Tx, s *sqliteio.Stmt, computerID string) (sqliteHostTurnRow, error) {
	if s.ColumnCount() != 10 {
		return sqliteHostTurnRow{}, failure("state_corrupt")
	}
	var texts [7]string
	for i := range texts {
		value, err := sqliteHostNormalizationText(s, i)
		if err != nil {
			return sqliteHostTurnRow{}, err
		}
		texts[i] = value
	}
	row := sqliteHostTurnRow{Key: texts[0], Source: texts[1], NativeSession: texts[2], Incarnation: texts[3], TurnID: texts[4], AgentID: texts[5], CWD: texts[6]}
	actorKind, err := s.Kind(7)
	if err != nil {
		return sqliteHostTurnRow{}, errors.Join(failure("state_corrupt"), err)
	}
	generationKind, err := s.Kind(8)
	if err != nil {
		return sqliteHostTurnRow{}, errors.Join(failure("state_corrupt"), err)
	}
	if actorKind == sqliteio.NullKind && generationKind == sqliteio.NullKind {
		// Retained actorless turns are valid without a current host session.
	} else if actorKind == sqliteio.TextKind && generationKind == sqliteio.BlobKind {
		key, err := sqliteHostNormalizationText(s, 7)
		if err != nil {
			return sqliteHostTurnRow{}, err
		}
		blob, err := s.Blob(8)
		if err != nil {
			return sqliteHostTurnRow{}, err
		}
		generation, err := sqliteDecodeUint64(blob)
		if err != nil {
			return sqliteHostTurnRow{}, err
		}
		ref, err := sqliteReadHostReceiptGeneration(tx, key, generation)
		if err != nil {
			return sqliteHostTurnRow{}, err
		}
		row.Actor = &ref
	} else {
		return sqliteHostTurnRow{}, failure("state_corrupt")
	}
	kind, err := s.Kind(9)
	if err != nil {
		return sqliteHostTurnRow{}, errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.IntegerKind {
		return sqliteHostTurnRow{}, failure("state_corrupt")
	}
	stopped, err := s.Int64(9)
	if err != nil {
		return sqliteHostTurnRow{}, err
	}
	if stopped != 0 && stopped != 1 {
		return sqliteHostTurnRow{}, failure("state_corrupt")
	}
	row.Stopped = stopped == 1
	if !sqliteValidHostTurn(computerID, row) {
		return sqliteHostTurnRow{}, failure("state_corrupt")
	}
	return row, nil
}

func sqliteHistoricalHostTurns(tx *sqliteio.Tx, computerID string, e HostEvent) ([]sqliteHostTurnRow, error) {
	if !sqliteValidHostTurnSelector(computerID, e) {
		return nil, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteHostTurnColumns+" FROM host_turns WHERE source=? AND native_session=? AND turn_id=? AND agent_id=?",
		sqliteio.Text(e.Source), sqliteio.Text(e.SessionID), sqliteio.Text(e.TurnID), sqliteio.Text(e.AgentID))
	if err != nil {
		return nil, err
	}
	return sqliteReadHostTurnCandidates(tx, s, computerID, e, false)
}

func sqliteToolHostTurns(tx *sqliteio.Tx, computerID string, e HostEvent) ([]sqliteHostTurnRow, error) {
	if !sqliteValidHostTurnSelector(computerID, e) {
		return nil, failure("validation")
	}
	if e.Source != "codex" || e.AgentID != "" {
		return sqliteHistoricalHostTurns(tx, computerID, e)
	}
	s, err := tx.Prepare("SELECT "+sqliteHostTurnColumns+" FROM host_turns WHERE source=? AND native_session=? AND turn_id=?",
		sqliteio.Text(e.Source), sqliteio.Text(e.SessionID), sqliteio.Text(e.TurnID))
	if err != nil {
		return nil, err
	}
	return sqliteReadHostTurnCandidates(tx, s, computerID, e, true)
}

func sqliteReadHostTurnCandidates(tx *sqliteio.Tx, s *sqliteio.Stmt, computerID string, e HostEvent, anyAgent bool) (result []sqliteHostTurnRow, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = []sqliteHostTurnRow{}
	seen := map[string]bool{}
	for {
		present, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !present {
			return result, nil
		}
		row, readErr := sqliteDecodeHostTurn(tx, s, computerID)
		if readErr != nil {
			return nil, readErr
		}
		// These comparisons stay raw, matching the legacy map iteration.
		if row.Source != e.Source || row.NativeSession != e.SessionID || row.TurnID != e.TurnID || !anyAgent && row.AgentID != e.AgentID || seen[row.Key] {
			return nil, failure("state_corrupt")
		}
		seen[row.Key] = true
		result = append(result, row)
	}
}

func sqliteValidHostTurnSelector(computerID string, e HostEvent) bool {
	return validUUID(computerID) && hostSource(e.Source) && safeIdentifier(e.SessionID, 256) && safeIdentifier(e.TurnID, 256) && (e.AgentID == "" || safeIdentifier(e.AgentID, 128))
}

func sqliteValidHostTurn(computerID string, row sqliteHostTurnRow) bool {
	return validUUID(computerID) && hostSource(row.Source) && safeIdentifier(row.NativeSession, 256) && validUUID(row.Incarnation) &&
		safeIdentifier(row.TurnID, 256) && (row.AgentID == "" || safeIdentifier(row.AgentID, 128)) && filepath.IsAbs(row.CWD) &&
		row.Key == hostTurnKey(row.Incarnation, HostEvent{TurnID: row.TurnID, AgentID: row.AgentID}) &&
		(row.Actor == nil || validRef(*row.Actor) && row.Actor.Key.ComputerID == computerID && row.Actor.Key.SessionID == row.Incarnation && row.Actor.Key.AgentID == hostAgent(row.AgentID))
}

func sqliteEncodeHostTurn(computerID string, row sqliteHostTurnRow) (sqliteEncodedHostTurn, error) {
	if !sqliteValidHostTurn(computerID, row) {
		return sqliteEncodedHostTurn{}, failure("validation")
	}
	texts := []string{row.Key, row.Source, row.NativeSession, row.Incarnation, row.TurnID, row.AgentID, row.CWD}
	for i := range texts {
		texts[i] = sqlitePersistClockString(texts[i])
	}
	encoded := sqliteEncodedHostTurn{key: texts[0], values: [10]sqliteio.Value{
		sqliteio.Text(texts[0]), sqliteio.Text(texts[1]), sqliteio.Text(texts[2]), sqliteio.Text(texts[3]), sqliteio.Text(texts[4]), sqliteio.Text(texts[5]), sqliteio.Text(texts[6]),
		sqliteio.Null(), sqliteio.Null(), sqliteMetaBool(row.Stopped),
	}}
	owned := sqliteHostTurnRow{Key: texts[0], Source: texts[1], NativeSession: texts[2], Incarnation: texts[3], TurnID: texts[4], AgentID: texts[5], CWD: texts[6], Stopped: row.Stopped}
	nulls := 2
	var blobs [][]byte
	if row.Actor != nil {
		actor, err := sqliteEncodeGeneration(*row.Actor)
		if err != nil {
			return sqliteEncodedHostTurn{}, err
		}
		generation, err := sqliteEncodeUint64(actor.ref.Generation)
		if err != nil {
			return sqliteEncodedHostTurn{}, err
		}
		encoded.values[7], encoded.values[8] = sqliteio.Text(actor.key), sqliteio.Blob(generation[:])
		owned.Actor = &actor.ref
		texts, blobs, nulls = append(texts, actor.key), [][]byte{generation[:]}, 0
	}
	if !sqliteValidHostTurn(computerID, owned) {
		return sqliteEncodedHostTurn{}, failure("validation")
	}
	var err error
	encoded.charge, err = sqliteRowCharge(1, nulls, texts, blobs)
	if err != nil {
		return sqliteEncodedHostTurn{}, err
	}
	return encoded, nil
}

func sqliteWriteHostTurn(tx *sqliteio.Tx, computerID string, before *sqliteHostTurnRow, after sqliteHostTurnRow) (delta int64, err error) {
	if before != nil && (before.Key != after.Key || before.Source != after.Source || before.NativeSession != after.NativeSession || before.Incarnation != after.Incarnation || before.TurnID != after.TurnID || before.AgentID != after.AgentID) {
		return 0, failure("validation")
	}
	next, err := sqliteEncodeHostTurn(computerID, after)
	if err != nil {
		return 0, err
	}
	var old sqliteEncodedHostTurn
	if before != nil {
		old, err = sqliteEncodeHostTurn(computerID, *before)
		if err != nil {
			return 0, err
		}
	}
	for _, row := range []*sqliteHostTurnRow{before, &after} {
		if row == nil || row.Actor == nil {
			continue
		}
		_, found, readErr := sqliteReadActorGeneration(tx, *row.Actor)
		if readErr != nil {
			return 0, readErr
		}
		if !found {
			return 0, failure("state_corrupt")
		}
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("INSERT INTO host_turns("+sqliteHostTurnColumns+") VALUES(?,?,?,?,?,?,?,?,?,?) RETURNING turn_key", next.values[:]...)
	} else {
		values := []sqliteio.Value{next.values[6], next.values[7], next.values[8], next.values[9]}
		values = append(values, old.values[:]...)
		s, err = tx.Prepare(`UPDATE host_turns SET cwd=?,actor_key=?,actor_generation=?,stopped=?
			WHERE turn_key=? AND source=? AND native_session=? AND incarnation=? AND turn_id=? AND agent_id=? AND cwd=?
			AND actor_key IS ? AND actor_generation IS ? AND stopped=? RETURNING turn_key`, values...)
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
	if err = sqliteHostNormalizationReturnedKeys(s, next.key); err != nil {
		return 0, err
	}
	return next.charge - old.charge, nil
}

// Shared by the three finite host projections; this does not coerce types.
func sqliteHostNormalizationText(s *sqliteio.Stmt, column int) (string, error) {
	kind, err := s.Kind(column)
	if err != nil {
		return "", errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.TextKind {
		return "", failure("state_corrupt")
	}
	value, err := s.Text(column)
	if err != nil {
		return "", err
	}
	if !utf8.ValidString(value) {
		return "", failure("state_corrupt")
	}
	return value, nil
}

func sqliteHostNormalizationReturnedKeys(s *sqliteio.Stmt, keys ...string) error {
	row, err := s.Step()
	if err != nil {
		return err
	}
	if !row || s.ColumnCount() != len(keys) {
		return failure("state_corrupt")
	}
	for i, key := range keys {
		value, err := sqliteHostNormalizationText(s, i)
		if err != nil {
			return err
		}
		if value != key {
			return failure("state_corrupt")
		}
	}
	if row, err = s.Step(); err != nil {
		return err
	} else if row {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteHostNormalizationWriteError(err error) error {
	var native *sqliteio.Error
	if errors.As(err, &native) && native.Category == sqliteio.Constraint && (native.Code == 1555 || native.Code == 2067) {
		return errors.Join(failure("validation"), err)
	}
	return err
}
