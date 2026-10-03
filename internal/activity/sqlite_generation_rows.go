//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sqliteGenerationColumns = "actor_key,generation,computer_id,source,session_id,agent_id"

type sqliteEncodedGeneration struct {
	ref    ActorRef
	key    string
	values [6]sqliteio.Value
	charge int64
}

func sqliteReadActorGeneration(tx *sqliteio.Tx, ref ActorRef) (result ActorRef, found bool, err error) {
	if !validRef(ref) {
		return result, false, failure("validation")
	}
	generation, err := sqliteEncodeUint64(ref.Generation)
	if err != nil {
		return result, false, err
	}
	key := actorKey(ref.Key)
	s, err := tx.Prepare("SELECT "+sqliteGenerationColumns+" FROM actor_generations WHERE actor_key=? AND generation=?",
		sqliteio.Text(key), sqliteio.Blob(generation[:]))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = ActorRef{}, false
		}
	}()
	row, err := s.Step()
	if err != nil || !row {
		return result, false, err
	}
	if s.ColumnCount() != 6 {
		return result, false, failure("state_corrupt")
	}
	var storedKey string
	for column := 0; column < 6; column++ {
		expected := sqliteio.TextKind
		if column == 1 {
			expected = sqliteio.BlobKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil || kind != expected {
			return result, false, failure("state_corrupt")
		}
		if column == 1 {
			value, readErr := s.Blob(column)
			if readErr != nil {
				return result, false, readErr
			}
			result.Generation, err = sqliteDecodeUint64(value)
			if err != nil {
				return result, false, err
			}
			continue
		}
		value, readErr := s.Text(column)
		if readErr != nil {
			return result, false, readErr
		}
		if !utf8.ValidString(value) {
			return result, false, failure("state_corrupt")
		}
		switch column {
		case 0:
			storedKey = value
		case 2:
			result.Key.ComputerID = value
		case 3:
			result.Key.Source = value
		case 4:
			result.Key.SessionID = value
		case 5:
			result.Key.AgentID = value
		}
	}
	materialized := ref
	materialized.Key.SessionID = sqlitePersistClockString(ref.Key.SessionID)
	materialized.Key.AgentID = sqlitePersistClockString(ref.Key.AgentID)
	if !validRef(result) || storedKey != actorKey(result.Key) || storedKey != key || result != materialized {
		return result, false, failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return result, false, err
	} else if row {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteEnsureActorGeneration(tx *sqliteio.Tx, ref ActorRef) (delta int64, err error) {
	// Preserve the raw actorKey lookup. In particular, do not repair a new raw
	// identity into a different existing identity before deciding whether it exists.
	_, found, err := sqliteReadActorGeneration(tx, ref)
	if err != nil || found {
		return 0, err
	}
	encoded, err := sqliteEncodeGeneration(ref)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO actor_generations("+sqliteGenerationColumns+") VALUES(?,?,?,?,?,?) RETURNING actor_key,generation", encoded.values[:]...)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			delta = 0
		}
	}()
	row, err := s.Step()
	if err != nil {
		return 0, err
	}
	if !row || s.ColumnCount() != 2 {
		return 0, failure("state_corrupt")
	}
	keyKind, keyErr := s.Kind(0)
	genKind, genErr := s.Kind(1)
	if keyErr != nil || genErr != nil || keyKind != sqliteio.TextKind || genKind != sqliteio.BlobKind {
		return 0, failure("state_corrupt")
	}
	key, err := s.Text(0)
	if err != nil {
		return 0, err
	}
	value, err := s.Blob(1)
	if err != nil {
		return 0, err
	}
	generation, err := sqliteDecodeUint64(value)
	if err != nil {
		return 0, err
	}
	if key != encoded.key || generation != encoded.ref.Generation {
		return 0, failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return 0, err
	} else if row {
		return 0, failure("state_corrupt")
	}
	return encoded.charge, nil
}

func sqliteGenerationCharge(ref ActorRef) (int64, error) {
	encoded, err := sqliteEncodeGeneration(ref)
	return encoded.charge, err
}

func sqliteEncodeGeneration(ref ActorRef) (sqliteEncodedGeneration, error) {
	if !validRef(ref) {
		return sqliteEncodedGeneration{}, failure("validation")
	}
	key := actorKey(ref.Key)
	materialized := ref
	materialized.Key.SessionID = sqlitePersistClockString(ref.Key.SessionID)
	materialized.Key.AgentID = sqlitePersistClockString(ref.Key.AgentID)
	// Legacy JSON can change malformed raw fields while retaining a different
	// escaped map key. Refuse this new identity instead of silently changing it.
	if !validRef(materialized) || actorKey(materialized.Key) != key {
		return sqliteEncodedGeneration{}, failure("validation")
	}
	generation, err := sqliteEncodeUint64(materialized.Generation)
	if err != nil {
		return sqliteEncodedGeneration{}, err
	}
	k := materialized.Key
	charge, err := sqliteRowCharge(0, 0, []string{key, k.ComputerID, k.Source, k.SessionID, k.AgentID}, [][]byte{generation[:]})
	if err != nil {
		return sqliteEncodedGeneration{}, err
	}
	return sqliteEncodedGeneration{ref: materialized, key: key, charge: charge, values: [6]sqliteio.Value{
		sqliteio.Text(key), sqliteio.Blob(generation[:]), sqliteio.Text(k.ComputerID),
		sqliteio.Text(k.Source), sqliteio.Text(k.SessionID), sqliteio.Text(k.AgentID),
	}}, nil
}
