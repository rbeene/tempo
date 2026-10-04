//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "github.com/rbeene/tempo/internal/activity/sqliteio"

type sqliteStatusOutbox struct {
	Attribution Attribution
	State       string
}

// Only owned status facts survive the read; no Tx or native owner is retained.
type sqliteStatusFacts struct {
	Meta          sqliteStoreMeta
	Actors        []Actor
	Segments      map[string]sqliteSegmentLocalRow
	Epochs        map[string]timelineEpoch
	Uncertainties []Uncertainty
	Reviews       []HostReceipt
	Intervals     []Interval
	Outbox        []sqliteStatusOutbox
}

func sqliteStatusRows(tx *sqliteio.Tx, meta sqliteStoreMeta) (sqliteStatusFacts, error) {
	f := sqliteStatusFacts{Meta: meta, Actors: []Actor{}, Segments: make(map[string]sqliteSegmentLocalRow),
		Epochs: make(map[string]timelineEpoch), Uncertainties: []Uncertainty{}, Reviews: []HostReceipt{},
		Intervals: []Interval{}, Outbox: []sqliteStatusOutbox{}}
	refs, err := sqliteStatusActorRefs(tx, meta.ComputerID)
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	selected := sqliteDependencySelection{}
	for _, ref := range refs {
		row, found, err := sqliteReadActorLocal(tx, meta.ComputerID, ref.Key)
		if err != nil {
			return sqliteStatusFacts{}, err
		}
		if !found || row.Ref != ref {
			return sqliteStatusFacts{}, failure("state_corrupt")
		}
		ids, err := sqliteActorUncertaintyIDsLocal(tx, meta.ComputerID, ref.Key)
		if err != nil {
			return sqliteStatusFacts{}, err
		}
		selected.ActorKeys = append(selected.ActorKeys, ref.Key)
		for ordinal := range ids {
			selected.ActorUncertaintyEdges = append(selected.ActorUncertaintyEdges, sqliteActorUncertaintySelection{Key: ref.Key, Ordinal: int64(ordinal)})
		}
		f.Actors = append(f.Actors, Actor{ID: row.ID, Revision: row.Revision, Ref: row.Ref, Sequence: row.Sequence,
			State: row.State, Health: row.Health, BindingID: row.BindingID, BindingRevision: row.BindingRevision,
			Attribution: row.Attribution, Parent: row.Parent, SegmentID: row.SegmentID, LastEvidence: row.LastEvidence, UncertaintyIDs: ids})
	}
	s, err := tx.Prepare("SELECT segment_id FROM segments WHERE finalized=0 ORDER BY segment_id")
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	selected.SegmentIDs, err = sqliteDependencyUUIDRows(s)
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	for _, id := range selected.SegmentIDs {
		row, found, err := sqliteReadSegmentLocal(tx, meta.ComputerID, id)
		if err != nil {
			return sqliteStatusFacts{}, err
		}
		if !found || row.Finalized {
			return sqliteStatusFacts{}, failure("state_corrupt")
		}
		f.Segments[id] = row
		if _, known := f.Epochs[row.EpochID]; !known {
			epoch, found, err := sqliteReadEpoch(tx, meta.ComputerID, row.EpochID)
			if err != nil {
				return sqliteStatusFacts{}, err
			}
			if !found {
				return sqliteStatusFacts{}, failure("state_corrupt")
			}
			f.Epochs[row.EpochID] = epoch.Value
		}
	}
	for _, actor := range f.Actors {
		if actor.SegmentID != nil {
			if _, found := f.Segments[*actor.SegmentID]; !found {
				return sqliteStatusFacts{}, failure("state_corrupt")
			}
		}
	}
	s, err = tx.Prepare("SELECT uncertainty_id FROM uncertainties ORDER BY uncertainty_id")
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	selected.UncertaintyIDs, err = sqliteDependencyUUIDRows(s)
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	for _, id := range selected.UncertaintyIDs {
		row, found, err := sqliteReadUncertaintyScalar(tx, meta.ComputerID, id)
		if err != nil {
			return sqliteStatusFacts{}, err
		}
		if !found {
			return sqliteStatusFacts{}, failure("state_corrupt")
		}
		f.Uncertainties = append(f.Uncertainties, row)
	}
	if err = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, meta.Revision, selected); err != nil {
		return sqliteStatusFacts{}, err
	}
	s, err = tx.Prepare("SELECT receipt_key FROM host_receipts ORDER BY receipt_key")
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	keys, err := sqliteCaptureIDs(s, true)
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	for _, key := range keys {
		row, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, key)
		if err != nil {
			return sqliteStatusFacts{}, err
		}
		if !found {
			return sqliteStatusFacts{}, failure("state_corrupt")
		}
		if captureReview(row.Record.Result) {
			f.Reviews = append(f.Reviews, row.Record.Result)
		}
	}
	s, err = tx.Prepare("SELECT interval_id FROM intervals ORDER BY creation_ordinal")
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	intervalIDs, err := sqliteDependencyUUIDRows(s)
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	s, err = tx.Prepare("SELECT id FROM outbox ORDER BY id")
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	rootIDs, err := sqliteDependencyUUIDRows(s)
	if err != nil {
		return sqliteStatusFacts{}, err
	}
	intervals := make(map[string]Interval, len(intervalIDs))
	for _, rootID := range rootIDs {
		item, found, err := sqliteReadSyncItem(tx, meta.ComputerID, rootID, meta.Revision)
		if err != nil {
			return sqliteStatusFacts{}, err
		}
		if !found {
			return sqliteStatusFacts{}, failure("state_corrupt")
		}
		if _, duplicate := intervals[item.Interval.ID]; duplicate {
			return sqliteStatusFacts{}, failure("state_corrupt")
		}
		intervals[item.Interval.ID] = item.Interval
		f.Outbox = append(f.Outbox, sqliteStatusOutbox{Attribution: item.Interval.Attribution, State: item.State})
	}
	if len(intervals) != len(intervalIDs) {
		return sqliteStatusFacts{}, failure("state_corrupt")
	}
	for _, id := range intervalIDs {
		interval, found := intervals[id]
		if !found {
			return sqliteStatusFacts{}, failure("state_corrupt")
		}
		f.Intervals = append(f.Intervals, interval)
	}
	// Empty history still checks the real singleton pending reservation. With
	// roots, each complete item already performs that same request-unit check.
	if len(rootIDs) == 0 {
		if err = sqliteValidateSelectedSync(tx, meta.ComputerID, meta.Revision, nil, nil); err != nil {
			return sqliteStatusFacts{}, err
		}
	}
	return f, nil
}

// Actor keys are opaque. Materialize the exact typed key/generation tuples,
// close that cursor, then resolve each real immutable generation identity.
func sqliteStatusActorRefs(tx *sqliteio.Tx, computer string) ([]ActorRef, error) {
	s, err := tx.Prepare("SELECT actor_key,generation FROM actors ORDER BY actor_key")
	if err != nil {
		return nil, err
	}
	identities, err := sqliteStatusActorIdentities(s)
	if err != nil {
		return nil, err
	}
	refs := make([]ActorRef, 0, len(identities))
	for _, identity := range identities {
		ref, err := sqliteReadHostReceiptGeneration(tx, identity[0], identity[1])
		if err != nil {
			return nil, err
		}
		if ref.Key.ComputerID != computer {
			return nil, failure("state_corrupt")
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

func sqliteStatusActorIdentities(s *sqliteio.Stmt) (result [][2]string, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([][2]string, 0)
	for {
		present, err := s.Step()
		if err != nil {
			return nil, err
		}
		if !present {
			return result, nil
		}
		if s.ColumnCount() != 2 {
			return nil, failure("state_corrupt")
		}
		key, err := sqliteDependencyText(s, 0)
		if err != nil {
			return nil, err
		}
		kind, err := s.Kind(1)
		if err != nil {
			return nil, err
		}
		if kind != sqliteio.BlobKind {
			return nil, failure("state_corrupt")
		}
		encoded, err := s.Blob(1)
		if err != nil {
			return nil, err
		}
		generation, err := sqliteDecodeUint64(encoded)
		if err != nil {
			return nil, err
		}
		if n, valid := counter(generation); !valid || n == 0 {
			return nil, failure("state_corrupt")
		}
		result = append(result, [2]string{key, generation})
	}
}
