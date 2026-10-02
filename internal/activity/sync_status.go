package activity

import (
	"context"
	"math/big"
	"sort"
	"strconv"
	"time"
)

func syncString(s string) *string { return &s }
func syncSortedItems(st *state) []OutboxItem {
	items := make([]OutboxItem, 0, len(st.Outbox))
	for _, o := range st.Outbox {
		items = append(items, o)
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if !a.Interval.Start.Equal(b.Interval.Start) {
			return a.Interval.Start.Before(b.Interval.Start)
		}
		return a.ID < b.ID
	})
	return items
}

type syncSum struct {
	exact, planned, confirmed       big.Int
	planComplete, confirmedComplete bool
}

func newSyncSum() *syncSum { return &syncSum{planComplete: true, confirmedComplete: true} }
func (s *syncSum) add(exact string, planned, confirmed *string) {
	n, _ := new(big.Int).SetString(exact, 10)
	if n != nil {
		s.exact.Add(&s.exact, n)
	}
	if planned == nil {
		s.planComplete = false
	} else if n, ok := new(big.Int).SetString(*planned, 10); ok {
		s.planned.Add(&s.planned, n)
	}
	if confirmed == nil {
		s.confirmedComplete = false
	} else if n, ok := new(big.Int).SetString(*confirmed, 10); ok {
		s.confirmed.Add(&s.confirmed, n)
	}
}
func (s *syncSum) totals() SyncTotals {
	r := SyncTotals{ExactDurationNS: s.exact.String()}
	if s.planComplete {
		r.PlannedDurationNS = syncString(s.planned.String())
		r.PlannedResidualNS = syncString(new(big.Int).Sub(&s.exact, &s.planned).String())
	}
	if s.confirmedComplete {
		r.ConfirmedDurationNS = syncString(s.confirmed.String())
		r.TotalResidualNS = syncString(new(big.Int).Sub(&s.exact, &s.confirmed).String())
	}
	return r
}
func (s *Service) SyncStatus(ctx context.Context) (SyncStatus, error) {
	st, _, e := s.store.read(ctx)
	if e != nil {
		return SyncStatus{}, e
	}
	r := SyncStatus{ContractVersion: 1, SnapshotRevision: st.Revision, Enabled: st.SyncEnabled, Configurations: []SyncConfiguration{}, Items: syncSortedItems(st), Worker: WorkerStatus{State: "not_installed", SyncEnabled: st.SyncEnabled}, Accounting: []SyncAccounting{}}
	keys := []string{}
	for k := range st.SyncConfigurations {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r.Configurations = append(r.Configurations, st.SyncConfigurations[k])
	}
	total := newSyncSum()
	groups := map[string]*syncSum{}
	views := map[string]SyncAccounting{}
	add := func(a Attribution, date *string, exact string, planned, confirmed *string) {
		total.add(exact, planned, confirmed)
		for _, day := range []*string{nil, date} {
			scope := "project"
			key := attributionKey("", a)
			if day != nil {
				scope = "day"
				key += "/" + *day
			}
			if groups[key] == nil {
				groups[key] = newSyncSum()
				views[key] = SyncAccounting{Scope: scope, Attribution: a, Date: day}
			}
			groups[key].add(exact, planned, confirmed)
			if date == nil {
				break
			}
		}
	}
	for _, o := range r.Items {
		switch o.State {
		case "queued":
			r.Worker.QueuedCount++
		case "unknown":
			r.Worker.UnknownCount++
		}
		if o.Plan == nil {
			loc, e := time.LoadLocation(o.Interval.Attribution.Timezone)
			if e != nil {
				return SyncStatus{}, failure("state_corrupt")
			}
			for cursor := o.Interval.Start; cursor.Before(o.Interval.End); {
				end := syncNextDay(cursor, loc)
				if end.IsZero() || !end.After(cursor) {
					return SyncStatus{}, failure("state_corrupt")
				}
				if end.After(o.Interval.End) {
					end = o.Interval.End
				}
				add(o.Interval.Attribution, syncString(cursor.In(loc).Format("2006-01-02")), strconv.FormatInt(int64(end.Sub(cursor)), 10), nil, nil)
				cursor = end
			}
			continue
		}
		for _, p := range o.Plan.Parts {
			add(o.Interval.Attribution, syncString(p.SpentDate), p.DurationNS, &p.PlannedDurationNS, p.ConfirmedDurationNS)
		}
	}
	r.Totals = total.totals()
	keys = keys[:0]
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := views[k]
		v.Totals = groups[k].totals()
		r.Accounting = append(r.Accounting, v)
	}
	return r, nil
}
