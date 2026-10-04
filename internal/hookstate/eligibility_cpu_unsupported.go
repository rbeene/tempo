//go:build !darwin && !linux

package hookstate

func eligibilityProcessCPU() ([2]int64, bool) { return [2]int64{}, false }
