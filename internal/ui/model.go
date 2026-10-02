// Package ui presents shared local activity and operations without owning state.
package ui

import (
	"encoding/json"
	"strconv"

	"github.com/rbeene/tempo/internal/activity"
)

// TimerKey is the stable identity of one project union timer.
type TimerKey struct{ ComputerID, AccountID, ProjectID string }

// Model retains read-only observations and presentation state. It has no clock,
// service, filesystem, or network access. Pending actions keep their own inputs.
type Model struct {
	snapshot          activity.ActivitySnapshot
	observed          bool
	sequence          uint64
	revision          uint64
	selected          int
	columns, rows     int
	stale             bool
	reason            string
	appearanceWarning string
}

func NewModel(columns, rows int) *Model { return &Model{columns: columns, rows: rows} }

func (m *Model) ApplySnapshot(requestSequence uint64, snapshot activity.ActivitySnapshot) bool {
	revision, valid := canonicalCounter(snapshot.SnapshotRevision)
	if requestSequence <= m.sequence || !valid || revision < m.revision || snapshot.ContractVersion != 1 {
		return false
	}
	// Own nested collections and optional pointers, so a reader may reuse its
	// returned snapshot without changing the frame or selected identity.
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return false
	}
	var owned activity.ActivitySnapshot
	if json.Unmarshal(encoded, &owned) != nil {
		return false
	}
	selected, present := m.SelectedKey()
	oldIndex := m.selected
	m.snapshot = owned
	m.observed, m.stale, m.reason = true, false, ""
	m.sequence, m.revision = requestSequence, revision
	m.selected = min(oldIndex, max(0, len(owned.ProjectTimers)-1))
	if present {
		for i, timer := range owned.ProjectTimers {
			if timerIdentity(timer) == selected {
				m.selected = i
				break
			}
		}
	}
	return true
}

func (m *Model) MarkStale(requestSequence uint64, reason string) {
	if requestSequence <= m.sequence {
		return
	}
	m.sequence, m.stale, m.reason = requestSequence, true, reason
}
func (m *Model) Resize(columns, rows int) { m.columns, m.rows = columns, rows }
func (m *Model) Move(delta int) {
	// Compare before adding to avoid an overflowing arbitrary input delta.
	if delta < -m.selected {
		m.selected = 0
		return
	}
	last := max(0, len(m.snapshot.ProjectTimers)-1)
	if delta > last-m.selected {
		m.selected = last
		return
	}
	m.selected += delta
}
func (m *Model) SelectedKey() (TimerKey, bool) {
	if len(m.snapshot.ProjectTimers) == 0 {
		return TimerKey{}, false
	}
	return timerIdentity(m.snapshot.ProjectTimers[m.selected]), true
}
func (m *Model) SnapshotRevision() string {
	if !m.observed {
		return ""
	}
	return m.snapshot.SnapshotRevision
}
func (m *Model) IsStale() bool { return m.stale }
func timerIdentity(timer activity.ProjectTimer) TimerKey {
	return TimerKey{timer.ComputerID, timer.AccountID, timer.ProjectID}
}
func canonicalCounter(s string) (uint64, bool) {
	if s == "" || len(s) > 1 && s[0] == '0' {
		return 0, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil
}
