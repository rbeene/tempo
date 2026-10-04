//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// A nonterminal checked close retains its owner privately. Read cleanup cannot
// create an uncertain write, but it must prevent a successful read result.
type sqliteBindingReadCleanupError struct {
	public   error
	evidence error
	owner    *sqliteio.Conn
}

func (e *sqliteBindingReadCleanupError) Error() string { return e.public.Error() }
func (e *sqliteBindingReadCleanupError) Unwrap() error { return e.public }

func (s *Service) listBindingsSQLite(ctx context.Context) (BindingList, error) {
	return s.sqliteBindingRead(ctx, "", nil, true)
}

func (s *Service) showBindingSQLite(ctx context.Context, in ShowBindingInput) (BindingList, error) {
	if in.BindingID != "" && (!validUUID(in.BindingID) || in.Path != "") {
		return BindingList{}, failure("validation")
	}
	var location *Location
	if in.BindingID == "" {
		loc, err := DiscoverLocation(ctx, in.Path)
		if err != nil {
			return BindingList{}, err
		}
		location = &loc
	}
	result, err := s.sqliteBindingRead(ctx, in.BindingID, location, false)
	if err != nil {
		return BindingList{}, err
	}
	if in.BindingID != "" {
		b := result.Bindings[0]
		if err := bindingAvailable(ctx, bindingRecord{Snapshot: BindingSnapshot{ID: b.ID, Revision: b.Revision, Attribution: b.Attribution}, Kind: b.Kind, Locator: b.Locator}); err != nil {
			return BindingList{}, err
		}
	}
	return result, nil
}

// sqliteBindingRead owns one checked native read. Git discovery and binding
// availability checks are outside the transaction; the list never probes paths.
func (s *Service) sqliteBindingRead(ctx context.Context, id string, location *Location, list bool) (result BindingList, err error) {
	if s == nil || s.store == nil || ctx == nil {
		return BindingList{}, failure("validation")
	}
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	directory, authority, database, err := sqliteLocation(s.store.path)
	if err != nil {
		return BindingList{}, err
	}
	budget := s.store.timeout
	if budget == 0 {
		budget = 250 * time.Millisecond
	}
	if budget < 0 || budget > time.Second {
		return BindingList{}, failure("validation")
	}
	deadline := sqliteLinkDeadline(ctx, budget)
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if cleanup != nil || owner != nil {
			err = &sqliteBindingReadCleanupError{public: failure("state_corrupt"), evidence: errors.Join(err, cleanup), owner: owner}
			result = BindingList{}
		} else if err != nil {
			err = sqliteCaptureError(err)
			result = BindingList{}
		}
	}()
	var kind sqliteio.LinkInspection
	c, kind, err = sqliteio.InspectForLink(ctx, directory, authority, database, deadline)
	if err != nil {
		return BindingList{}, err
	}
	result = BindingList{ContractVersion: 1, SnapshotRevision: "0", Bindings: []Binding{}}
	if kind == sqliteio.LinkAbsent || kind == sqliteio.LinkPristine {
		if c != nil {
			return BindingList{}, failure("state_corrupt")
		}
		if list {
			return result, nil
		}
		return BindingList{}, failure("not_found")
	}
	if kind != sqliteio.LinkWAL || c == nil {
		return BindingList{}, failure("state_corrupt")
	}
	tx, err = c.Begin(ctx, sqliteio.Read)
	if err != nil {
		return BindingList{}, err
	}
	meta, current, err := sqliteReadLinkSchema(tx, authority, database)
	if err != nil {
		return BindingList{}, err
	}
	if !current {
		if list {
			return result, nil
		}
		return BindingList{}, failure("not_found")
	}
	result.SnapshotRevision = meta.Revision
	if list {
		result.Bindings, err = sqliteActiveBindingViews(tx, meta)
	} else {
		var row sqliteBindingRow
		var found bool
		if id != "" {
			row, found, err = sqliteReadBinding(tx, meta.ComputerID, id)
		} else {
			var selected *sqliteBindingRow
			selected, _, err = sqliteCaptureLocation(tx, meta.ComputerID, *location)
			if selected != nil {
				row, found = *selected, true
			}
		}
		if err == nil && found && row.Record != nil && !row.Record.Deleted {
			var view Binding
			view, err = sqliteBindingReadView(tx, meta, row)
			if err == nil {
				result.Bindings = append(result.Bindings, view)
			}
		} else if err == nil {
			err = failure("not_found")
		}
	}
	if err != nil {
		return BindingList{}, err
	}
	if err = ctx.Err(); err != nil {
		return BindingList{}, err
	}
	return result, nil
}

func sqliteBindingReadView(tx *sqliteio.Tx, meta sqliteStoreMeta, row sqliteBindingRow) (Binding, error) {
	if row.Record == nil || row.Record.Deleted || row.ComputerID != meta.ComputerID {
		return Binding{}, failure("state_corrupt")
	}
	refs, err := sqliteLinkAttached(tx, meta, row)
	if err != nil {
		return Binding{}, err
	}
	return Binding{ID: row.Snapshot.ID, Revision: row.Snapshot.Revision, Kind: row.Record.Kind, Locator: row.Record.Locator, Attribution: row.Snapshot.Attribution, AttachedActors: refs}, nil
}

func sqliteActiveBindingViews(tx *sqliteio.Tx, meta sqliteStoreMeta) ([]Binding, error) {
	s, err := tx.Prepare("SELECT binding_id FROM bindings WHERE computer_id=? AND record_present=1 AND active=1 ORDER BY binding_id", sqliteio.Text(meta.ComputerID))
	if err != nil {
		return nil, err
	}
	ids, err := sqliteDependencyUUIDRows(s)
	if err != nil {
		return nil, err
	}
	bindings := make([]Binding, 0, len(ids))
	previous := ""
	for _, id := range ids {
		if id <= previous {
			return nil, failure("state_corrupt")
		}
		previous = id
		row, found, err := sqliteReadBinding(tx, meta.ComputerID, id)
		if err != nil {
			return nil, err
		}
		if !found || row.Record == nil || row.Record.Deleted {
			return nil, failure("state_corrupt")
		}
		view, err := sqliteBindingReadView(tx, meta, row)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, view)
	}
	return bindings, nil
}
