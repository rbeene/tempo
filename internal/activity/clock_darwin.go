//go:build darwin && cgo

package activity

/*
#include <mach/mach_time.h>
#include <sys/sysctl.h>
#include <sys/time.h>
#include <stdint.h>
static int tempo_sample(uint64_t *elapsed, uint64_t *awake, long long *sec, long long *usec) {
 struct timeval boot;
 size_t len = sizeof(boot);
 mach_timebase_info_data_t info;
 if (sysctlbyname("kern.boottime", &boot, &len, NULL, 0) != 0 ||
     mach_timebase_info(&info) != KERN_SUCCESS || info.denom == 0) return -1;
 *elapsed = (uint64_t)(((__uint128_t)mach_continuous_time() * info.numer) / info.denom);
 *awake = (uint64_t)(((__uint128_t)mach_absolute_time() * info.numer) / info.denom);
 *sec = boot.tv_sec;
 *usec = boot.tv_usec;
 return 0;
}
*/
import "C"

import (
	"errors"
	"fmt"
)

func platformSample() (string, uint64, uint64, error) {
	var elapsed, awake C.uint64_t
	var sec, usec C.longlong
	if C.tempo_sample(&elapsed, &awake, &sec, &usec) != 0 {
		return "", 0, 0, errors.New("clock unavailable")
	}
	return fmt.Sprintf("darwin-boot-%d-%d", int64(sec), int64(usec)), uint64(elapsed), uint64(awake), nil
}
