package activity

import (
	"strconv"
	"time"
)

type nativeClock struct{}

func (nativeClock) Sample() (ClockSample, error) {
	epoch, elapsed, awake, err := platformSample()
	s := ClockSample{Capability: "unavailable", WallUTC: time.Now().UTC()}
	if err != nil {
		return s, &Error{Code: "clock_unavailable", Message: "reliable local clock evidence is unavailable"}
	}
	s.Capability = "available"
	s.Epoch = &epoch
	e, a := strconv.FormatUint(elapsed, 10), strconv.FormatUint(awake, 10)
	s.ElapsedNS, s.AwakeNS = &e, &a
	return s, nil
}
