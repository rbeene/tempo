//go:build linux

package activity

import (
	"errors"
	"math"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

func platformSample() (string, uint64, uint64, error) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", 0, 0, err
	}
	epoch := strings.TrimSpace(string(b))
	if len(epoch) != 36 {
		return "", 0, 0, errors.New("invalid boot epoch")
	}
	read := func(id uintptr) (uint64, error) {
		var ts syscall.Timespec
		_, _, errno := syscall.Syscall(syscall.SYS_CLOCK_GETTIME, id, uintptr(unsafe.Pointer(&ts)), 0)
		if errno != 0 {
			return 0, errno
		}
		if ts.Sec < 0 || ts.Nsec < 0 || ts.Nsec >= 1e9 || uint64(ts.Sec) > math.MaxUint64/1_000_000_000 {
			return 0, errors.New("invalid clock counter")
		}
		return uint64(ts.Sec)*1e9 + uint64(ts.Nsec), nil
	}
	// CLOCK_BOOTTIME includes suspend, CLOCK_MONOTONIC excludes it.
	elapsed, err := read(7)
	if err != nil {
		return "", 0, 0, err
	}
	awake, err := read(1)
	if err != nil {
		return "", 0, 0, err
	}
	return "linux-boot-" + epoch, elapsed, awake, nil
}
