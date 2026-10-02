package activity

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/rbeene/tempo/internal/identity"
)

func safeIdentifier(s string, max int) bool {
	if s == "" || len(s) > max {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func validKey(k ActorKey) bool {
	return validUUID(k.ComputerID) && (k.Source == "codex" || k.Source == "claude" || k.Source == "manual-test") && safeIdentifier(k.SessionID, 256) && safeIdentifier(k.AgentID, 256)
}
func validRef(r ActorRef) bool { n, ok := counter(r.Generation); return validKey(r.Key) && ok && n > 0 }
func validAttribution(a Attribution) bool {
	if !identity.Valid(a.AccountID) || !identity.Valid(a.UserID) || !identity.Valid(a.ProjectID) || !identity.Valid(a.TaskID) || !safeIdentifier(a.Timezone, 128) || a.Timezone == "Local" {
		return false
	}
	_, err := time.LoadLocation(a.Timezone)
	return err == nil
}
func validateEvent(e Event) error {
	if e.ContractVersion != 1 {
		return failure("unsupported_contract")
	}
	g, ok := counter(e.Generation)
	q, ok2 := counter(e.Sequence)
	if !validKey(e.Actor) || !ok || !ok2 || g == 0 || q == 0 || q == math.MaxUint64 || g == math.MaxUint64 || !safeIdentifier(e.EventID, 256) {
		return failure("validation")
	}
	switch e.Kind {
	case "work", "wait_user", "wait_permission", "wait_children", "interrupt", "finish", "observe_work":
	default:
		return failure("validation")
	}
	if e.BindingID != "" && !validUUID(e.BindingID) {
		return failure("validation")
	}
	if e.BindingRevision != "" {
		if _, ok := counter(e.BindingRevision); !ok {
			return failure("validation")
		}
	}
	if (e.BindingID == "") != (e.BindingRevision == "") {
		return failure("validation")
	}
	if e.Parent != nil && (!validRef(*e.Parent) || e.Parent.Key.ComputerID != e.Actor.ComputerID) {
		return failure("validation")
	}
	if e.CWD != "" && (!filepath.IsAbs(e.CWD) || !safeIdentifier(e.CWD, 4096)) {
		return failure("validation")
	}
	return nil
}

// DecodeEvent validates the finite allowlisted ingress shape before fingerprinting.
func DecodeEvent(r io.Reader) (Event, error) {
	b, err := io.ReadAll(io.LimitReader(r, 16*1024+1))
	if err != nil || len(b) > 16*1024 || !strictJSON(b) {
		return Event{}, failure("validation")
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(b, &raw) != nil || raw == nil {
		return Event{}, failure("validation")
	}
	check := func(obj map[string]json.RawMessage, allowed string) bool {
		for k := range obj {
			if !strings.Contains(" "+allowed+" ", " "+k+" ") {
				return false
			}
		}
		return true
	}
	if !check(raw, "contract_version actor generation sequence event_id kind binding_id binding_revision parent cwd") {
		return Event{}, failure("validation")
	}
	keyOK := func(b json.RawMessage) bool {
		var k map[string]json.RawMessage
		return json.Unmarshal(b, &k) == nil && k != nil && check(k, "computer_id source session_id agent_id")
	}
	if !keyOK(raw["actor"]) {
		return Event{}, failure("validation")
	}
	if v, ok := raw["parent"]; ok && string(v) != "null" {
		var p map[string]json.RawMessage
		if json.Unmarshal(v, &p) != nil || !check(p, "key generation") || !keyOK(p["key"]) {
			return Event{}, failure("validation")
		}
	}
	var e Event
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&e) != nil {
		return Event{}, failure("validation")
	}
	return e, validateEvent(e)
}
