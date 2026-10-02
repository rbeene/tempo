package themes

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/rbeene/tempo/internal/activity"
)

const maxStateBytes = 4 << 20

type intent struct {
	Theme      string `json:"theme"`
	IfRevision string `json:"if_revision"`
}

type receipt struct {
	Intent      intent                  `json:"intent"`
	Fingerprint string                  `json:"fingerprint"`
	Result      activity.MutationResult `json:"result"`
}

type state struct {
	FormatVersion      int                `json:"format_version"`
	Revision           string             `json:"snapshot_revision"`
	PreferenceRevision string             `json:"preference_revision"`
	Theme              string             `json:"selected_theme"`
	Requests           map[string]receipt `json:"requests"`
}

func emptyState() *state {
	return &state{FormatVersion: 1, Revision: "0", PreferenceRevision: "0", Theme: "terminal-default", Requests: map[string]receipt{}}
}

func fingerprint(input intent) string {
	b, _ := json.Marshal(input)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Receipts form a bounded complete history of preference transactions. Check
// it independently of current selection so damaged historical replay data can
// never silently restore the wrong result or authorize another selection.
func validState(st *state) bool {
	if st.FormatVersion != 1 || st.Requests == nil {
		return false
	}
	transactions, ok := counter(st.Revision)
	if !ok || transactions != uint64(len(st.Requests)) {
		return false
	}
	preference, ok := counter(st.PreferenceRevision)
	if !ok || preference > transactions {
		return false
	}
	ordered := make([]*receipt, len(st.Requests))
	for id, r := range st.Requests {
		tx, valid := counter(r.Result.SnapshotRevision)
		if !validUUID(id) || !valid || tx == 0 || tx > transactions || ordered[tx-1] != nil ||
			r.Fingerprint != fingerprint(r.Intent) || r.Result.ContractVersion != 1 || r.Result.RequestID != id ||
			r.Result.EntityRevision == nil || r.Result.AffectedIDs == nil || len(r.Result.AffectedIDs) != 0 {
			return false
		}
		if _, err := Lookup(r.Intent.Theme); err != nil {
			return false
		}
		ordered[tx-1] = &r
	}
	selected, revision := "terminal-default", "0"
	for _, r := range ordered {
		if r == nil || (r.Intent.IfRevision != "" && r.Intent.IfRevision != revision) {
			return false
		}
		changed := selected != r.Intent.Theme
		if r.Result.Changed != changed {
			return false
		}
		if changed {
			selected, revision = r.Intent.Theme, bump(revision)
		}
		if *r.Result.EntityRevision != revision {
			return false
		}
	}
	return st.Theme == selected && st.PreferenceRevision == revision
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

func newID() string {
	var b [16]byte
	// Go's cryptographic random reader fails closed if entropy is unavailable.
	_, _ = rand.Read(b[:])
	b[6], b[8] = (b[6]&15)|64, (b[8]&63)|128
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func counter(s string) (uint64, bool) {
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil && strconv.FormatUint(n, 10) == s
}

func bump(s string) string { n, _ := counter(s); return strconv.FormatUint(n+1, 10) }

func failure(code string) *Error {
	message := "local preference operation failed"
	switch code {
	case "validation":
		message = "invalid appearance preference input"
	case "state_corrupt":
		message = "appearance preferences are unsafe or corrupt; preserve them for review"
	case "unsupported_contract":
		message = "appearance preference format is newer than this version supports"
	case "state_busy":
		message = "appearance preferences are busy"
	case "revision_conflict":
		message = "appearance preference changed; reread before applying a new choice"
	case "request_conflict":
		message = "request identity was already used for different appearance input"
	case "local_write_unknown":
		message = "appearance write durability is unknown; retry with the same request identity and input"
	}
	return &Error{Code: code, Message: message, Retryable: code == "state_busy", Uncertain: code == "local_write_unknown"}
}
