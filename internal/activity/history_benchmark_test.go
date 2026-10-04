package activity

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// This historical JSON reducer benchmark retains real receipts/evidence without
// doing thousands of setup fsyncs. The timed request reads, validates, reduces and replaces the whole
// persisted history through the public service, as a hook process does.
func BenchmarkIngestRetainedHistory(b *testing.B) {
	for _, size := range []int{1000, 5000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			var tick int64
			epoch := "benchmark-boot"
			clock := ClockFunc(func() (ClockSample, error) {
				v := strconv.FormatInt(tick*int64(time.Second), 10)
				return ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC).Add(time.Duration(tick) * time.Second), Epoch: &epoch, ElapsedNS: &v, AwakeNS: &v}, nil
			})
			binding := BindingSnapshot{ID: qaBindingA, Revision: "1", Attribution: Attribution{AccountID: "1", UserID: "2", ProjectID: "3", TaskID: "4", Timezone: "UTC"}}
			path := filepath.Join(b.TempDir(), "state", "state.json")
			service := qaLegacyNew(Options{Path: path, Clock: clock, ResolveBinding: func(context.Context, Event) (BindingSnapshot, bool, error) { return binding, true, nil }})
			err := service.store.update(context.Background(), func(st *state) (bool, error) {
				st.ComputerID = qaComputer
				st.Bindings[binding.ID] = binding
				for i := 1; i <= size; i++ {
					tick = int64(i)
					e := qaEvent("history", "1", strconv.Itoa(i), "observe_work", "")
					if i == 1 {
						e.Kind = "work"
						e.BindingID = binding.ID
						e.BindingRevision = "1"
					}
					if _, _, err := service.reduce(context.Background(), st, e); err != nil {
						return false, err
					}
				}
				return true, nil
			})
			if err != nil {
				b.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tick++
				e := qaEvent("history", "1", strconv.FormatInt(tick, 10), "observe_work", "")
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := service.Ingest(ctx, e)
				cancel()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(info.Size()), "state_bytes")
		})
	}
}
