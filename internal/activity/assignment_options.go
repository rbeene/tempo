package activity

import (
	"context"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/identity"
	"net/url"
)

type AssignedTask struct{ ID, Name string }
type AssignedProject struct {
	ID, Name, ClientID, ClientName string
	Tasks                          []AssignedTask
}
type AssignmentCatalog struct {
	AccountID, UserID string
	Projects          []AssignedProject
}

func label(o harvest.Object) string { v, _ := o["name"].(string); return v }
func DiscoverAssignments(ctx context.Context, account string, api harvest.Provider) (AssignmentCatalog, error) {
	empty := AssignmentCatalog{}
	if !identity.Valid(account) {
		return empty, failure("validation")
	}
	if api == nil {
		return empty, failure("auth")
	}
	accounts, e := api.Accounts(ctx)
	if e != nil {
		return empty, e
	}
	found := false
	for _, a := range accounts {
		if bindingID(a) == account && a["product"] == "harvest" {
			found = true
		}
	}
	if !found {
		return empty, failure("forbidden")
	}
	user, e := api.Get(ctx, "/users/me")
	if e != nil {
		return empty, e
	}
	uid := bindingID(user)
	if !identity.Valid(uid) {
		return empty, failure("response")
	}
	active, ok := user["is_active"].(bool)
	if !ok {
		return empty, failure("response")
	}
	if !active {
		return empty, failure("forbidden")
	}
	rows, e := api.List(ctx, "/users/me/project_assignments", url.Values{})
	if e != nil {
		return empty, e
	}
	result := AssignmentCatalog{AccountID: account, UserID: uid, Projects: []AssignedProject{}}
	seen := map[string]bool{}
	for _, row := range rows {
		project := bindingObject(row, "project")
		id := bindingID(project)
		if !identity.Valid(id) || seen[id] {
			return empty, failure("response")
		}
		seen[id] = true
		if row["is_active"] != true || project["is_active"] == false {
			continue
		}
		tasks, ok := row["task_assignments"].([]any)
		if !ok {
			return empty, failure("response")
		}
		client := bindingObject(row, "client")
		p := AssignedProject{ID: id, Name: label(project), ClientID: bindingID(client), ClientName: label(client), Tasks: []AssignedTask{}}
		seenTasks := map[string]bool{}
		for _, raw := range tasks {
			t, ok := raw.(map[string]any)
			if !ok {
				return empty, failure("response")
			}
			task := bindingObject(t, "task")
			tid := bindingID(task)
			if !identity.Valid(tid) {
				return empty, failure("response")
			}
			if t["is_active"] == true && task["is_active"] != false && !seenTasks[tid] {
				p.Tasks = append(p.Tasks, AssignedTask{tid, label(task)})
				seenTasks[tid] = true
			}
		}
		result.Projects = append(result.Projects, p)
	}
	return result, nil
}
