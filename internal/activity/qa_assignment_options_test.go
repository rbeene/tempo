package activity

import (
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/harvest"
	"testing"
)

func TestQAAssignmentOptionsShareNestedActiveRules(t *testing.T) {
	p := qaNewLinkProvider(t)
	p.assignments = []harvest.Object{
		{"is_active": true, "project": harvest.Object{"id": "3", "name": "Same"}, "client": harvest.Object{"id": "8", "name": "Client A"}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": "4", "name": "Work"}}, harvest.Object{"is_active": true, "task": harvest.Object{"id": "5", "is_active": false}}, harvest.Object{"is_active": false, "task": harvest.Object{"id": "6"}}}},
		{"is_active": true, "project": harvest.Object{"id": "7", "name": "Same", "is_active": false}, "task_assignments": []any{}},
		{"is_active": false, "project": harvest.Object{"id": "9", "name": "Same"}, "task_assignments": []any{}},
	}
	r, e := DiscoverAssignments(context.Background(), "1", p)
	if e != nil {
		t.Fatal(e)
	}
	if r.AccountID != "1" || r.UserID != "2" || len(r.Projects) != 1 || r.Projects[0].ID != "3" || r.Projects[0].ClientID != "8" || len(r.Projects[0].Tasks) != 1 || r.Projects[0].Tasks[0].ID != "4" {
		t.Fatalf("bad assignment choices %+v", r)
	}
}
func TestQAAssignmentOptionsRejectPartialCatalog(t *testing.T) {
	p := qaNewLinkProvider(t)
	p.listErr = errors.New("synthetic upstream detail")
	r, e := DiscoverAssignments(context.Background(), "1", p)
	if e == nil || len(r.Projects) != 0 {
		t.Fatalf("partial choices exposed %+v %v", r, e)
	}
}
func TestQAAssignmentOptionsAccountAndUserScope(t *testing.T) {
	for _, bad := range []string{"account", "user"} {
		t.Run(bad, func(t *testing.T) {
			p := qaNewLinkProvider(t)
			if bad == "account" {
				p.accounts = []harvest.Object{{"id": "1", "product": "forecast"}}
			} else {
				p.user["is_active"] = false
			}
			r, e := DiscoverAssignments(context.Background(), "1", p)
			if e == nil || len(r.Projects) != 0 {
				t.Fatalf("unauthorized choices %+v %v", r, e)
			}
		})
	}
}
