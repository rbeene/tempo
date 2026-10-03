//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteActorUncertaintySelection struct {
	Key     ActorKey
	Ordinal int64
}

type sqliteSegmentEventSelection struct {
	SegmentID string
	Ordinal   int64
}

type sqliteHostSessionIdentity struct {
	Source, NativeSession string
}

type sqliteHostToolIdentity struct {
	TurnKey, ToolID string
}

type sqliteDependencySelection struct {
	ActorKeys                                            []ActorKey
	SegmentIDs, UncertaintyIDs, EpochIDs                 []string
	ActorUncertaintyEdges                                []sqliteActorUncertaintySelection
	SegmentEventEdges                                    []sqliteSegmentEventSelection
	HostSessions                                         []sqliteHostSessionIdentity
	HostTurnKeys                                         []string
	HostTools                                            []sqliteHostToolIdentity
	HostReceiptKeys, EventReceiptKeys, ResolveRequestIDs []string
	ChangedSegmentIDs, ChangedUncertaintyIDs             []string
	ChangedHostTurnKeys, ChangedResolveRequestIDs        []string
}

// Only the two cyclic row families need forward queues. A read dependency never
// enters a Changed set, so it cannot start a new reverse-owner expansion.
type sqliteCaptureDependencyQueue struct {
	segments, uncertainties      []string
	segmentSeen, uncertaintySeen map[string]bool
}

func (q *sqliteCaptureDependencyQueue) addSegment(id string) {
	if !q.segmentSeen[id] {
		q.segmentSeen[id] = true
		q.segments = append(q.segments, id)
	}
}

func (q *sqliteCaptureDependencyQueue) addUncertainty(id string) {
	if !q.uncertaintySeen[id] {
		q.uncertaintySeen[id] = true
		q.uncertainties = append(q.uncertainties, id)
	}
}

func sqliteValidateSelectedCaptureDependencies(tx *sqliteio.Tx, computer, revisionCeiling string, selected sqliteDependencySelection) error {
	if !sqliteDependencyScope(computer, revisionCeiling) {
		return failure("validation")
	}
	queue := sqliteCaptureDependencyQueue{segmentSeen: make(map[string]bool), uncertaintySeen: make(map[string]bool)}
	actors := make(map[ActorKey]bool)
	edges := make(map[sqliteActorUncertaintySelection]bool)
	events := make(map[sqliteSegmentEventSelection]bool)
	epochs := make(map[string]bool)
	sessions := make(map[sqliteHostSessionIdentity]bool)
	turns := make(map[string]bool)
	tools := make(map[sqliteHostToolIdentity]bool)
	hostReceipts := make(map[string]bool)
	eventReceipts := make(map[string]bool)
	requests := make(map[string]bool)
	changedSegments := make(map[string]bool)
	changedUncertainties := make(map[string]bool)
	changedTurns := make(map[string]bool)
	changedRequests := make(map[string]bool)
	for _, key := range selected.ActorKeys {
		actors[key] = true
	}
	for _, id := range selected.SegmentIDs {
		queue.addSegment(id)
	}
	for _, id := range selected.UncertaintyIDs {
		queue.addUncertainty(id)
	}
	for _, id := range selected.EpochIDs {
		epochs[id] = true
	}
	for _, edge := range selected.ActorUncertaintyEdges {
		edges[edge] = true
	}
	for _, edge := range selected.SegmentEventEdges {
		events[edge] = true
	}
	for _, identity := range selected.HostSessions {
		sessions[identity] = true
	}
	for _, key := range selected.HostTurnKeys {
		turns[key] = true
	}
	for _, identity := range selected.HostTools {
		tools[identity] = true
	}
	for _, key := range selected.HostReceiptKeys {
		hostReceipts[key] = true
	}
	for _, key := range selected.EventReceiptKeys {
		eventReceipts[key] = true
	}
	for _, id := range selected.ResolveRequestIDs {
		requests[id] = true
	}
	// These unions precede every reverse lookup, including empty result sets.
	for _, id := range selected.ChangedSegmentIDs {
		queue.addSegment(id)
		changedSegments[id] = true
	}
	for _, id := range selected.ChangedUncertaintyIDs {
		queue.addUncertainty(id)
		changedUncertainties[id] = true
	}
	for _, key := range selected.ChangedHostTurnKeys {
		turns[key] = true
		changedTurns[key] = true
	}
	for _, id := range selected.ChangedResolveRequestIDs {
		requests[id] = true
		changedRequests[id] = true
	}
	for id := range changedSegments {
		if !validUUID(id) {
			return failure("validation")
		}
		owners, err := sqliteDependencySegmentActors(tx, computer, id)
		if err != nil {
			return err
		}
		for _, key := range owners {
			actors[key] = true
		}
		uncertainties, err := sqliteDependencySegmentUncertainties(tx, id)
		if err != nil {
			return err
		}
		for _, uncertainty := range uncertainties {
			queue.addUncertainty(uncertainty)
		}
	}
	for id := range changedUncertainties {
		if !validUUID(id) {
			return failure("validation")
		}
		segments, err := sqliteDependencyUncertaintySegments(tx, id)
		if err != nil {
			return err
		}
		for _, segment := range segments {
			queue.addSegment(segment)
		}
		memberships, err := sqliteDependencyUncertaintyActors(tx, computer, id)
		if err != nil {
			return err
		}
		for _, edge := range memberships {
			edges[edge] = true
		}
	}
	for key := range changedTurns {
		if len(key) != 64 {
			return failure("validation")
		}
		owners, err := sqliteDependencyRootSessions(tx, key)
		if err != nil {
			return err
		}
		for _, identity := range owners {
			sessions[identity] = true
		}
	}
	for id := range changedRequests {
		if !validUUID(id) {
			return failure("validation")
		}
		owners, err := sqliteDependencyRequestUncertainties(tx, id)
		if err != nil {
			return err
		}
		for _, uncertainty := range owners {
			queue.addUncertainty(uncertainty)
		}
	}
	for edge := range edges {
		id, err := sqliteActorUncertaintyDependency(tx, computer, edge.Key, edge.Ordinal)
		if err != nil {
			return err
		}
		actors[edge.Key] = true
		queue.addUncertainty(id)
	}
	for edge := range events {
		if !validUUID(edge.SegmentID) || edge.Ordinal < 0 {
			return failure("validation")
		}
		if _, err := sqliteDependencyEdge(tx, edge.SegmentID, edge.Ordinal, false); err != nil {
			return err
		}
		queue.addSegment(edge.SegmentID)
	}
	for key := range actors {
		row, found, err := sqliteReadActorLocal(tx, computer, key)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
		if err := sqliteValidateActorDependencies(tx, computer, row); err != nil {
			return err
		}
		if row.SegmentID != nil {
			queue.addSegment(*row.SegmentID)
		}
	}
	// Each newly reached segment/uncertainty is checked exactly once as an
	// owner. Peer scalar reads do not recurse back into these validators.
	segmentIndex, uncertaintyIndex := 0, 0
	for segmentIndex < len(queue.segments) || uncertaintyIndex < len(queue.uncertainties) {
		if segmentIndex < len(queue.segments) {
			id := queue.segments[segmentIndex]
			segmentIndex++
			row, found, err := sqliteReadSegmentLocal(tx, computer, id)
			if err != nil {
				return err
			}
			if !found {
				return failure("state_corrupt")
			}
			if err := sqliteValidateSegmentDependencies(tx, computer, row); err != nil {
				return err
			}
			if row.UncertaintyID != nil {
				queue.addUncertainty(*row.UncertaintyID)
			}
		}
		if uncertaintyIndex < len(queue.uncertainties) {
			id := queue.uncertainties[uncertaintyIndex]
			uncertaintyIndex++
			row, found, err := sqliteReadUncertaintyScalar(tx, computer, id)
			if err != nil {
				return err
			}
			if !found {
				return failure("state_corrupt")
			}
			if err := sqliteValidateUncertaintyDependencies(tx, computer, revisionCeiling, row); err != nil {
				return err
			}
			queue.addSegment(row.SegmentID)
		}
	}
	for id := range epochs {
		_, found, err := sqliteReadEpoch(tx, computer, id)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
	}
	for identity := range sessions {
		_, found, err := sqliteReadHostSession(tx, computer, identity.Source, identity.NativeSession)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
	}
	for key := range turns {
		_, found, err := sqliteReadHostTurn(tx, computer, key)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
	}
	for identity := range tools {
		_, found, err := sqliteReadHostTool(tx, computer, identity.TurnKey, identity.ToolID)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
	}
	for key := range hostReceipts {
		_, found, err := sqliteReadHostReceipt(tx, computer, revisionCeiling, key)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
	}
	for key := range eventReceipts {
		if err := sqliteValidateEventReceiptID(tx, key, revisionCeiling); err != nil {
			return err
		}
	}
	for id := range requests {
		row, found, err := sqliteReadResolveRequestProof(tx, id, revisionCeiling)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
		if err := sqliteValidateResolveRequestDependencies(tx, computer, revisionCeiling, row); err != nil {
			return err
		}
	}
	return nil
}

// D05 resolves the actual immutable six-column generation identity. It never
// reconstructs an ActorKey by parsing a stored actor_key string.
func sqliteDependencySegmentActors(tx *sqliteio.Tx, computer, id string) (result []ActorKey, err error) {
	s, err := tx.Prepare("SELECT actor_key,generation FROM actors WHERE segment_id=?", sqliteio.Text(id))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	for {
		present, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !present {
			return result, nil
		}
		if s.ColumnCount() != 2 {
			return nil, failure("state_corrupt")
		}
		ref, readErr := sqliteDependencyStoredActor(tx, s, computer)
		if readErr != nil {
			return nil, readErr
		}
		result = append(result, ref.Key)
	}
}

func sqliteDependencySegmentUncertainties(tx *sqliteio.Tx, id string) ([]string, error) {
	s, err := tx.Prepare("SELECT uncertainty_id FROM uncertainties WHERE segment_id=?", sqliteio.Text(id))
	if err != nil {
		return nil, err
	}
	return sqliteDependencyUUIDRows(s)
}

func sqliteDependencyUncertaintySegments(tx *sqliteio.Tx, id string) ([]string, error) {
	s, err := tx.Prepare("SELECT segment_id FROM segments WHERE uncertainty_id=?", sqliteio.Text(id))
	if err != nil {
		return nil, err
	}
	return sqliteDependencyUUIDRows(s)
}

func sqliteDependencyRequestUncertainties(tx *sqliteio.Tx, id string) ([]string, error) {
	s, err := tx.Prepare("SELECT uncertainty_id FROM recovery_decisions WHERE request_id=?", sqliteio.Text(id))
	if err != nil {
		return nil, err
	}
	return sqliteDependencyUUIDRows(s)
}

// D06, D07 and D09 share only their exact one-UUID projection/statement owner.
func sqliteDependencyUUIDRows(s *sqliteio.Stmt) (result []string, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	for {
		present, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !present {
			return result, nil
		}
		if s.ColumnCount() != 1 {
			return nil, failure("state_corrupt")
		}
		id, readErr := sqliteDependencyText(s, 0)
		if readErr != nil {
			return nil, readErr
		}
		if !validUUID(id) {
			return nil, failure("state_corrupt")
		}
		result = append(result, id)
	}
}

// The LEFT JOIN retains orphan memberships as NULL generation corruption.
// Every matching ordinal is decoded before the composer deduplicates identities.
func sqliteDependencyUncertaintyActors(tx *sqliteio.Tx, computer, id string) (result []sqliteActorUncertaintySelection, err error) {
	s, err := tx.Prepare("SELECT c.actor_key,a.generation,c.ordinal,c.uncertainty_id FROM actor_uncertainties AS c LEFT JOIN actors AS a ON a.actor_key=c.actor_key WHERE c.uncertainty_id=?", sqliteio.Text(id))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	for {
		present, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !present {
			return result, nil
		}
		if s.ColumnCount() != 4 {
			return nil, failure("state_corrupt")
		}
		ref, readErr := sqliteDependencyStoredActor(tx, s, computer)
		if readErr != nil {
			return nil, readErr
		}
		kind, kindErr := s.Kind(2)
		if kindErr != nil {
			return nil, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind != sqliteio.IntegerKind {
			return nil, failure("state_corrupt")
		}
		ordinal, readErr := s.Int64(2)
		if readErr != nil {
			return nil, readErr
		}
		uncertainty, readErr := sqliteDependencyText(s, 3)
		if readErr != nil {
			return nil, readErr
		}
		if ordinal < 0 || uncertainty != id {
			return nil, failure("state_corrupt")
		}
		result = append(result, sqliteActorUncertaintySelection{Key: ref.Key, Ordinal: ordinal})
	}
}

func sqliteDependencyStoredActor(tx *sqliteio.Tx, s *sqliteio.Stmt, computer string) (ActorRef, error) {
	key, err := sqliteDependencyText(s, 0)
	if err != nil {
		return ActorRef{}, err
	}
	kind, err := s.Kind(1)
	if err != nil {
		return ActorRef{}, errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.BlobKind {
		return ActorRef{}, failure("state_corrupt")
	}
	encoded, err := s.Blob(1)
	if err != nil {
		return ActorRef{}, err
	}
	generation, err := sqliteDecodeUint64(encoded)
	if err != nil {
		return ActorRef{}, err
	}
	ref, err := sqliteReadHostReceiptGeneration(tx, key, generation)
	if err != nil {
		return ActorRef{}, err
	}
	if ref.Key.ComputerID != computer {
		return ActorRef{}, failure("state_corrupt")
	}
	return ref, nil
}

func sqliteDependencyRootSessions(tx *sqliteio.Tx, key string) (result []sqliteHostSessionIdentity, err error) {
	s, err := tx.Prepare("SELECT source,native_session,session_key FROM host_sessions WHERE root_turn_key=? AND root_turn_key<>''", sqliteio.Text(key))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	for {
		present, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !present {
			return result, nil
		}
		if s.ColumnCount() != 3 {
			return nil, failure("state_corrupt")
		}
		source, readErr := sqliteDependencyText(s, 0)
		if readErr != nil {
			return nil, readErr
		}
		native, readErr := sqliteDependencyText(s, 1)
		if readErr != nil {
			return nil, readErr
		}
		storedKey, readErr := sqliteDependencyText(s, 2)
		if readErr != nil {
			return nil, readErr
		}
		if !hostSource(source) || !safeIdentifier(native, 256) || storedKey != hostSessionKey(HostEvent{Source: source, SessionID: native}) {
			return nil, failure("state_corrupt")
		}
		result = append(result, sqliteHostSessionIdentity{Source: source, NativeSession: native})
	}
}
