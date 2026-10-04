//go:build darwin || linux

package hookstate

import "golang.org/x/sys/unix"

// Two read-only process snapshots per observed Eligibility, never per chunk.
func eligibilityProcessCPU() ([2]int64, bool) {
	var usage unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &usage) != nil {
		return [2]int64{}, false
	}
	seconds := [2]int64{int64(usage.Utime.Sec), int64(usage.Stime.Sec)}
	micros := [2]int64{int64(usage.Utime.Usec), int64(usage.Stime.Usec)}
	var value [2]int64
	for i := range value {
		if seconds[i] < 0 || seconds[i] > (1<<63-1)/1000000 || micros[i] < 0 || micros[i] >= 1000000 {
			return [2]int64{}, false
		}
		value[i] = seconds[i] * 1000000
		if value[i] > (1<<63-1)-micros[i] {
			return [2]int64{}, false
		}
		value[i] += micros[i]
	}
	return value, true
}
