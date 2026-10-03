package activity

import "time"

// normalizeActivitySample is the shared scalar boundary, with no clock callback.
func normalizeActivitySample(value ClockSample, sampleErr error, zeroWallFallback time.Time) (ClockSample, error) {
	if value.WallUTC.IsZero() {
		value.WallUTC = zeroWallFallback
	}
	value.WallUTC = value.WallUTC.UTC()
	if _, _, ok := sampleValues(value); sampleErr != nil || !ok {
		value.Capability = "unavailable"
		value.Epoch, value.ElapsedNS, value.AwakeNS = nil, nil, nil
		return value, failure("clock_unavailable")
	}
	return value, nil
}
