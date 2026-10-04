//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "time"

type sqliteCaptureClockMode uint8

const (
	sqliteCaptureNative sqliteCaptureClockMode = iota
	sqliteCapturePrepared
)

type sqliteCaptureClockSite uint8

const (
	sqliteCaptureClockNone sqliteCaptureClockSite = iota
	sqliteCaptureClockGap
	sqliteCaptureClockReduce
)

type sqliteCaptureSample struct {
	Value       ClockSample
	Unavailable bool
}
type sqliteCaptureClock struct {
	Mode     sqliteCaptureClockMode
	Site     sqliteCaptureClockSite
	Prepared *sqliteCaptureSample
	Consumed bool
}

func sqliteCaptureCopyString(p *string) *string {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
func sqliteCaptureCopySample(v ClockSample) ClockSample {
	v.Epoch = sqliteCaptureCopyString(v.Epoch)
	v.ElapsedNS = sqliteCaptureCopyString(v.ElapsedNS)
	v.AwakeNS = sqliteCaptureCopyString(v.AwakeNS)
	return v
}
func (clock *sqliteCaptureClock) take(site sqliteCaptureClockSite) (ClockSample, error) {
	if clock == nil || clock.Consumed || site == sqliteCaptureClockNone || site != clock.Site || (site != sqliteCaptureClockGap && site != sqliteCaptureClockReduce) {
		return ClockSample{}, failure("validation")
	}
	switch clock.Mode {
	case sqliteCaptureNative:
		if clock.Prepared != nil {
			return ClockSample{}, failure("validation")
		}
		clock.Consumed = true
		value, err := (nativeClock{}).Sample()
		var fallback time.Time
		if value.WallUTC.IsZero() {
			fallback = time.Now().UTC()
		}
		return normalizeActivitySample(value, err, fallback)
	case sqliteCapturePrepared:
		if clock.Prepared == nil {
			return ClockSample{}, failure("validation")
		}
		clock.Consumed = true
		value := sqliteCaptureCopySample(clock.Prepared.Value)
		if clock.Prepared.Unavailable {
			return value, failure("clock_unavailable")
		}
		return value, nil
	default:
		return ClockSample{}, failure("validation")
	}
}
