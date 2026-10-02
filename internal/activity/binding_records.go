package activity

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
)

type bindingRecord struct {
	Snapshot BindingSnapshot `json:"snapshot"`
	Kind     string          `json:"kind"`
	Locator  string          `json:"locator"`
	Deleted  bool            `json:"deleted"`
}

func bindingView(st *state, r bindingRecord) Binding {
	refs := []ActorRef{}
	for _, a := range st.Actors {
		if a.BindingID == r.Snapshot.ID && !terminal(a) {
			refs = append(refs, a.Ref)
		}
	}
	sort.Slice(refs, func(i, j int) bool { return actorKey(refs[i].Key) < actorKey(refs[j].Key) })
	return Binding{ID: r.Snapshot.ID, Revision: r.Snapshot.Revision, Kind: r.Kind, Locator: r.Locator, Attribution: r.Snapshot.Attribution, AttachedActors: refs}
}
func exactRecord(st *state, loc Location) (bindingRecord, bool) {
	for _, r := range st.BindingRecords {
		if !r.Deleted && r.Kind == loc.Kind && r.Locator == loc.Locator {
			return r, true
		}
	}
	return bindingRecord{}, false
}

func recordForLocation(st *state, loc Location) (bindingRecord, bool) {
	if loc.Kind == "repository" {
		return exactRecord(st, loc)
	}
	var best bindingRecord
	found := false
	for _, r := range st.BindingRecords {
		if !r.Deleted && r.Kind == "directory" && containsPath(r.Locator, loc.Path) && (!found || len(r.Locator) > len(best.Locator)) {
			best = r
			found = true
		}
	}
	return best, found
}

func bindingAvailable(ctx context.Context, r bindingRecord) error {
	if r.Deleted {
		return failure("binding_unavailable")
	}
	if r.Kind == "directory" {
		loc, err := DiscoverLocation(ctx, r.Locator)
		if err != nil {
			return err
		}
		if loc.Kind != "directory" || loc.Locator != r.Locator {
			return failure("binding_unavailable")
		}
		return nil
	}
	canonical, err := canonicalDirectory(r.Locator)
	if err != nil || canonical != r.Locator {
		return failure("binding_unavailable")
	}
	out, _, err := gitProbe(ctx, filepath.Dir(r.Locator), "--git-dir="+r.Locator, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return failure("binding_unavailable")
	}
	got := strings.TrimSuffix(out, "\n")
	if !filepath.IsAbs(got) {
		got = filepath.Join(filepath.Dir(r.Locator), got)
	}
	got, err = canonicalDirectory(got)
	if err != nil || got != r.Locator {
		return failure("binding_unavailable")
	}
	return nil
}
