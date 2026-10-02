package activity

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
)

type hostSession struct {
	RootTurn string `json:"root_turn"`
	ID       string `json:"id"`
	Source   string `json:"source"`
	NativeID string `json:"native_id"`
	CWD      string `json:"cwd"`
}
type hostTurn struct {
	Source    string              `json:"source"`
	SessionID string              `json:"session_id"`
	Tools     map[string]hostTool `json:"tools,omitempty"`
	Session   string              `json:"session"`
	TurnID    string              `json:"turn_id"`
	AgentID   string              `json:"agent_id"`
	CWD       string              `json:"cwd"`
	Actor     *ActorRef           `json:"actor"`
	Stopped   bool                `json:"stopped"`
}
type hostTool struct {
	Name  string `json:"name"`
	Phase string `json:"phase"`
}
type hostReceiptRecord struct {
	Fingerprint string      `json:"fingerprint"`
	Result      HostReceipt `json:"result"`
	ErrorCode   string      `json:"error_code"`
}

func hostHash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func hostSessionKey(e HostEvent) string { return hostHash([]string{e.Source, e.SessionID}) }
func hostTurnKey(session string, e HostEvent) string {
	return hostHash([]string{session, e.TurnID, e.AgentID})
}
func hostAgent(id string) string {
	if id == "" {
		return "root"
	}
	return "child:" + base64.RawURLEncoding.EncodeToString([]byte(id))
}
func hostEventKey(session string, e HostEvent) string {
	return hostHash([]string{session, e.TurnID, e.AgentID, e.Kind, e.ToolID, e.SessionSource})
}
func hostFingerprint(e HostEvent) string { e.CWD = ""; return hostHash(e) }
func initHostState(st *state) {
	if st.HostSessions == nil {
		st.HostSessions = map[string]*hostSession{}
	}
	if st.HostTurns == nil {
		st.HostTurns = map[string]*hostTurn{}
	}
	if st.HostReceipts == nil {
		st.HostReceipts = map[string]hostReceiptRecord{}
	}
}
func validHostState(st *state) bool {
	for key, s := range st.HostSessions {
		if s == nil || !validUUID(s.ID) || s.Source != "codex" || !safeIdentifier(s.NativeID, 256) || key != hostSessionKey(HostEvent{Source: s.Source, SessionID: s.NativeID}) || !filepath.IsAbs(s.CWD) {
			return false
		}
	}
	for key, t := range st.HostTurns {
		if t == nil || t.Source != "codex" || !safeIdentifier(t.SessionID, 256) || !validUUID(t.Session) || !safeIdentifier(t.TurnID, 256) || t.AgentID != "" && !safeIdentifier(t.AgentID, 128) || !filepath.IsAbs(t.CWD) || key != hostTurnKey(t.Session, HostEvent{TurnID: t.TurnID, AgentID: t.AgentID}) {
			return false
		}
		for id, tool := range t.Tools {
			if !safeIdentifier(id, 256) || !safeIdentifier(tool.Name, 256) || tool.Phase != "pre" && tool.Phase != "post" {
				return false
			}
		}
		if t.Actor != nil && (!validRef(*t.Actor) || t.Actor.Key.ComputerID != st.ComputerID || t.Actor.Key.SessionID != t.Session || t.Actor.Key.AgentID != hostAgent(t.AgentID)) {
			return false
		}
	}
	for key, r := range st.HostReceipts {
		result := r.Result
		rev, ok := counter(result.SnapshotRevision)
		current, _ := counter(st.Revision)
		if len(key) != 64 || len(r.Fingerprint) != 64 || result.ContractVersion != 1 || !validUUID(result.ID) || result.Source != "codex" || !safeIdentifier(result.SessionID, 256) || !ok || rev == 0 || rev > current || result.Durability != "committed" || result.Origin != "unverified" || result.ObservedAt.IsZero() {
			return false
		}
		if result.Actor != nil && (!validRef(*result.Actor) || result.Actor.Key.ComputerID != st.ComputerID) {
			return false
		}
		switch result.Disposition {
		case "applied", "stale", "review_required":
		default:
			return false
		}
		switch result.Ordering {
		case "supported", "review_required", "unavailable":
		default:
			return false
		}
		if result.ProfileBasis == "operator_declared" {
			if len(result.Fingerprint) != 64 {
				return false
			}
			if n, ok := counter(result.ProfileRevision); !ok || n == 0 {
				return false
			}
		} else if result.ProfileBasis != "none" || result.ProfileRevision != "0" || result.Fingerprint != "" || result.Disposition != "review_required" {
			return false
		}
		switch r.ErrorCode {
		case "", "clock_unavailable", "clock_conflict", "event_gap", "event_conflict", "invalid_transition":
		default:
			return false
		}
	}
	return true
}
