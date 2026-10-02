package activity

import (
	"encoding/json"
	"math"
	"strconv"
	"time"
)

type segment struct {
	ID              string          `json:"id"`
	Actor           ActorRef        `json:"actor"`
	Binding         BindingSnapshot `json:"binding"`
	EpochID         string          `json:"epoch_id"`
	StartSample     ClockSample     `json:"start_sample"`
	ConfirmedSample ClockSample     `json:"confirmed_sample"`
	Start           time.Time       `json:"start"`
	Confirmed       time.Time       `json:"confirmed"`
	End             *time.Time      `json:"end"`
	UncertaintyID   *string         `json:"uncertainty_id"`
	EventReferences []string        `json:"event_references"`
	Finalized       bool            `json:"finalized"`
}
type timelineEpoch struct {
	ID          string      `json:"id"`
	ComputerID  string      `json:"computer_id"`
	Attribution Attribution `json:"attribution"`
	Anchor      ClockSample `json:"anchor"`
}
type eventReceipt struct {
	Fingerprint string      `json:"fingerprint"`
	Result      EventResult `json:"result"`
}
type uncertaintyEvidence struct {
	Detection      ClockSample  `json:"detection"`
	LastConfirmed  ClockSample  `json:"last_confirmed"`
	BoundSample    *ClockSample `json:"bound_sample"`
	MissingFrom    string       `json:"missing_from,omitempty"`
	MissingThrough string       `json:"missing_through,omitempty"`
}

func actorKey(k ActorKey) string { b, _ := json.Marshal(k); return string(b) }
func eventKey(e Event) string    { return actorKey(e.Actor) + "/" + e.Generation + "/" + e.Sequence }
func timerKey(computer string, a Attribution) string {
	return computer + "/" + a.AccountID + "/" + a.ProjectID
}
func sampleValues(s ClockSample) (uint64, uint64, bool) {
	if s.Capability != "available" || s.WallUTC.IsZero() || s.Epoch == nil || !safeIdentifier(*s.Epoch, 256) || s.ElapsedNS == nil || s.AwakeNS == nil {
		return 0, 0, false
	}
	e, ok := counter(*s.ElapsedNS)
	a, ok2 := counter(*s.AwakeNS)
	return e, a, ok && ok2 && e <= math.MaxInt64 && a <= math.MaxInt64
}
func discontinuity(a, b ClockSample) string {
	ea, aa, ok := sampleValues(a)
	eb, ab, ok2 := sampleValues(b)
	if !ok || !ok2 {
		return "restart_unknown"
	}
	if *a.Epoch != *b.Epoch {
		return "restart_unknown"
	}
	if eb < ea || ab < aa {
		return "clock_changed"
	}
	elapsed, awake := time.Duration(eb-ea), time.Duration(ab-aa)
	if absDuration(elapsed-awake) > time.Second {
		return "suspend"
	}
	if absDuration(b.WallUTC.Sub(a.WallUTC)-elapsed) > time.Second {
		return "clock_changed"
	}
	return ""
}
func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
func projectSample(ep *timelineEpoch, s ClockSample) (time.Time, bool) {
	if ep == nil || discontinuity(ep.Anchor, s) != "" {
		return time.Time{}, false
	}
	a, _, _ := sampleValues(ep.Anchor)
	b, _, _ := sampleValues(s)
	return ep.Anchor.WallUTC.Add(time.Duration(b - a)), true
}
func selectEpoch(st *state, computer string, a Attribution, s ClockSample) *timelineEpoch {
	// A project shares one elapsed coordinate system even across binding IDs.
	for i := len(st.Epochs) - 1; i >= 0; i-- {
		ep := st.Epochs[i]
		if ep.ComputerID == computer && ep.Attribution == a {
			if _, ok := projectSample(ep, s); ok {
				return ep
			}
			break
		}
	}
	ep := &timelineEpoch{ID: newID(), ComputerID: computer, Attribution: a, Anchor: s}
	st.Epochs = append(st.Epochs, ep)
	return ep
}
func findEpoch(st *state, id string) *timelineEpoch {
	for _, ep := range st.Epochs {
		if ep.ID == id {
			return ep
		}
	}
	return nil
}
func boundTime(st *state, a *Actor, s ClockSample) time.Time {
	if a.SegmentID != nil {
		if seg := st.Segments[*a.SegmentID]; seg != nil {
			if t, ok := projectSample(findEpoch(st, seg.EpochID), s); ok {
				return t
			}
		}
	}
	return s.WallUTC
}
func terminal(a *Actor) bool { return a.State == "finished" || a.State == "interrupted" }
func quarantine(st *state, a *Actor, reason string, s ClockSample) bool {
	if hostPendingWait(st, a) {
		return retainHostWaitLoss(st, a, "SourceObservation", s.WallUTC)
	}
	if a.State != "working" || a.Health != "continuous" || a.SegmentID == nil {
		return false
	}
	seg := st.Segments[*a.SegmentID]
	if seg == nil {
		return false
	}
	id := newID()
	u := &Uncertainty{ID: id, Revision: "1", Actor: a.Ref, SegmentID: seg.ID, Attribution: a.Attribution, LowerBound: seg.Confirmed, Reason: reason, State: "unresolved"}
	st.Uncertainties[id] = u
	st.UncertaintyEvidence[id] = uncertaintyEvidence{Detection: s, LastConfirmed: seg.ConfirmedSample}
	seg.UncertaintyID = &id
	a.UncertaintyIDs = append(a.UncertaintyIDs, id)
	a.Health = "stale"
	a.Revision = bump(a.Revision)
	return true
}
func capUncertainties(st *state, a *Actor, s ClockSample) error {
	end := boundTime(st, a, s)
	for _, id := range a.UncertaintyIDs {
		u := st.Uncertainties[id]
		if u == nil || u.State != "unresolved" || u.UpperBound != nil {
			continue
		}
		if end.Before(u.LowerBound) {
			return failure("clock_conflict")
		}
		t := end
		u.UpperBound = &t
		u.Revision = bump(u.Revision)
		ev := st.UncertaintyEvidence[id]
		sample := s
		ev.BoundSample = &sample
		st.UncertaintyEvidence[id] = ev
	}
	return nil
}
func durationString(d time.Duration) string { return strconv.FormatInt(int64(d), 10) }

func actorDiscontinuity(st *state, a *Actor, s ClockSample) string {
	if why := discontinuity(a.LastEvidence, s); why != "" {
		return why
	}
	if a.SegmentID != nil {
		if seg := st.Segments[*a.SegmentID]; seg != nil {
			if ep := findEpoch(st, seg.EpochID); ep != nil {
				return discontinuity(ep.Anchor, s)
			}
		}
	}
	return ""
}

// Clock evidence belongs to the computer, not just the callback's actor. A later
// recovered clock cannot erase a failure already observed by another project.
func quarantineClock(st *state, s ClockSample) bool {
	changed := false
	for _, a := range st.Actors {
		if a.Health != "continuous" {
			continue
		}
		if why := actorDiscontinuity(st, a, s); why != "" {
			if hostPendingWait(st, a) {
				changed = retainHostWaitLoss(st, a, "ClockObservation", s.WallUTC) || changed
			} else {
				changed = quarantine(st, a, why, s) || changed
			}
		}
	}
	return changed
}
