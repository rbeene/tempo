package activity

import (
	"encoding/hex"
	"path/filepath"
	"strings"
)

func validStoredLocation(kind, locator string) bool {
	return (kind == "repository" || kind == "directory") && filepath.IsAbs(locator) && filepath.Clean(locator) == locator && safeIdentifier(locator, 4096)
}
func validBindingView(b Binding, computer string) bool {
	if !validBinding(BindingSnapshot{ID: b.ID, Revision: b.Revision, Attribution: b.Attribution}) || !validStoredLocation(b.Kind, b.Locator) || b.AttachedActors == nil {
		return false
	}
	seen := map[ActorRef]bool{}
	for _, a := range b.AttachedActors {
		if !validRef(a) || a.Key.ComputerID != computer || seen[a] {
			return false
		}
		seen[a] = true
	}
	return true
}
func validBindingState(st *state) bool {
	locators := map[string]bool{}
	for id, r := range st.BindingRecords {
		if id != r.Snapshot.ID || !validBinding(r.Snapshot) || !validStoredLocation(r.Kind, r.Locator) {
			return false
		}
		b, live := st.Bindings[id]
		if r.Deleted {
			if live {
				return false
			}
		} else {
			key := r.Kind + ":" + r.Locator
			if !live || b != r.Snapshot || locators[key] {
				return false
			}
			locators[key] = true
		}
	}
	revision, _ := counter(st.Revision)
	for id, r := range st.Requests {
		if !validUUID(id) || len(r.Fingerprint) != 64 || strings.ToLower(r.Fingerprint) != r.Fingerprint {
			return false
		}
		if _, err := hex.DecodeString(r.Fingerprint); err != nil {
			return false
		}
		if recoveryOperation(r.Operation) {
			if !validRecoveryReceipt(st, id, r, revision) {
				return false
			}
			continue
		}
		if r.Error != nil {
			return false
		}
		switch r.Operation {
		case "bindings.link", "bindings.repair":
			b := r.BindingResult
			if b == nil || r.MutationResult != nil || b.ContractVersion != 1 || b.RequestID != id || !validBindingView(b.Binding, st.ComputerID) {
				return false
			}
			if n, ok := counter(b.SnapshotRevision); !ok || n == 0 || n > revision {
				return false
			}
			current, ok := st.BindingRecords[b.Binding.ID]
			if !ok {
				return false
			}
			old, _ := counter(b.Binding.Revision)
			now, _ := counter(current.Snapshot.Revision)
			if old > now {
				return false
			}
		case "bindings.unlink":
			m := r.MutationResult
			if m == nil || r.BindingResult != nil || m.ContractVersion != 1 || m.RequestID != id || !m.Changed || len(m.AffectedIDs) != 1 || m.EntityRevision == nil {
				return false
			}
			if n, ok := counter(m.SnapshotRevision); !ok || n == 0 || n > revision {
				return false
			}
			entity, ok := counter(*m.EntityRevision)
			if !ok || entity == 0 {
				return false
			}
			for _, id := range m.AffectedIDs {
				record, ok := st.BindingRecords[id]
				if !validUUID(id) || !ok || !record.Deleted {
					return false
				}
				n, _ := counter(record.Snapshot.Revision)
				if entity > n {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}
