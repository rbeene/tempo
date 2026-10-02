package activity

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
)

const stateVersion = 1
const maxStateBytes = 32 << 20

type state struct {
	SyncEnabled         bool                           `json:"sync_enabled,omitempty"`
	SyncConfigurations  map[string]SyncConfiguration   `json:"sync_configurations,omitempty"`
	HostSessions        map[string]*hostSession        `json:"host_sessions,omitempty"`
	HostTurns           map[string]*hostTurn           `json:"host_turns,omitempty"`
	HostReceipts        map[string]hostReceiptRecord   `json:"host_receipts,omitempty"`
	RecoveryDecisions   map[string]recoveryDecision    `json:"recovery_decisions,omitempty"`
	Requests            map[string]mutationRequest     `json:"requests,omitempty"`
	BindingRecords      map[string]bindingRecord       `json:"binding_records,omitempty"`
	SchemaVersion       int                            `json:"schema_version"`
	ComputerID          string                         `json:"computer_id"`
	Revision            string                         `json:"revision"`
	Bindings            map[string]BindingSnapshot     `json:"bindings"`
	Actors              map[string]*Actor              `json:"actors"`
	Uncertainties       map[string]*Uncertainty        `json:"uncertainties"`
	Intervals           []Interval                     `json:"intervals"`
	Outbox              map[string]OutboxItem          `json:"outbox"`
	Segments            map[string]*segment            `json:"segments"`
	Receipts            map[string]eventReceipt        `json:"receipts"`
	EventIDs            map[string]string              `json:"event_ids"`
	Epochs              []*timelineEpoch               `json:"epochs"`
	UncertaintyEvidence map[string]uncertaintyEvidence `json:"uncertainty_evidence"`
}

func emptyState() *state {
	return &state{Segments: map[string]*segment{}, Receipts: map[string]eventReceipt{}, EventIDs: map[string]string{}, Epochs: []*timelineEpoch{}, UncertaintyEvidence: map[string]uncertaintyEvidence{}, SchemaVersion: stateVersion, Revision: "0", Bindings: map[string]BindingSnapshot{}, Actors: map[string]*Actor{}, Uncertainties: map[string]*Uncertainty{}, Intervals: []Interval{}, Outbox: map[string]OutboxItem{}}
}
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("secure randomness unavailable")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
func validUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func counter(s string) (uint64, bool) {
	if s == "" || len(s) > 1 && s[0] == '0' {
		return 0, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, e := strconv.ParseUint(s, 10, 64)
	return n, e == nil
}
func bump(s string) string { n, _ := counter(s); return strconv.FormatUint(n+1, 10) }
func failure(code string) *Error {
	messages := map[string]string{"state_corrupt": "local activity state is unsafe or corrupt; preserve it for review", "state_busy": "local activity state is busy", "local_write_unknown": "local write durability is unknown; retry with the same identity", "validation": "invalid local activity input", "clock_unavailable": "reliable local clock evidence is unavailable"}
	m := messages[code]
	if m == "" {
		m = "local activity operation failed"
	}
	return &Error{Code: code, Message: m, Retryable: code == "state_busy", Uncertain: code == "local_write_unknown"}
}

func attributionConflict(a, b Attribution) *Error {
	fields := []string{}
	if a.UserID != b.UserID {
		fields = append(fields, "user_id")
	}
	if a.TaskID != b.TaskID {
		fields = append(fields, "task_id")
	}
	if a.Timezone != b.Timezone {
		fields = append(fields, "timezone")
	}
	e := failure("attribution_conflict")
	e.Details = map[string]any{"fields": fields}
	return e
}
