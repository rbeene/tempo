package activity

import "time"

func syncZoneMatches(remote, local string) bool {
	if mapped, ok := syncHarvestZones[remote]; ok {
		remote = mapped
	} else {
		found := false
		for _, zone := range syncHarvestZones {
			if remote == zone {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if local == "UTC" {
		local = "Etc/UTC"
	}
	return remote == local
}
func syncTimestampSafe(start, end time.Time, loc *time.Location) bool {
	a, b := start.In(loc), end.In(loc)
	if !end.After(start) || a.Second() != 0 || a.Nanosecond() != 0 || b.Second() != 0 || b.Nanosecond() != 0 || a.Format("2006-01-02") != b.Format("2006-01-02") {
		return false
	}
	_, transition := a.ZoneBounds()
	if !transition.IsZero() && !transition.After(end) {
		return false
	}
	return !syncAmbiguous(a) && !syncAmbiguous(b)
}

// Enumerate nearby zone periods and invert the wall time under each actual
// offset. A second valid instant is a fold even when both endpoints share offset.
func syncAmbiguous(at time.Time) bool {
	loc := at.Location()
	y, m, d := at.Date()
	h, min, sec := at.Clock()
	wall := time.Date(y, m, d, h, min, sec, at.Nanosecond(), time.UTC)
	cursor, limit := at.Add(-48*time.Hour), at.Add(48*time.Hour)
	for i := 0; i < 16 && cursor.Before(limit); i++ {
		current := cursor.In(loc)
		_, offset := current.Zone()
		candidate := wall.Add(-time.Duration(offset) * time.Second).In(loc)
		if !candidate.Equal(at) && candidate.Format("2006-01-02T15:04:05.999999999") == at.Format("2006-01-02T15:04:05.999999999") {
			return true
		}
		_, next := current.ZoneBounds()
		if next.IsZero() || !next.After(cursor) {
			break
		}
		cursor = next
	}
	return false
}
