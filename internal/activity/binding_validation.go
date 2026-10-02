package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/identity"
	"net/url"
	"strings"
	"time"
)

func required(fields ...string) *Error {
	e := failure("input_required")
	e.Message = "explicit linking choices are required"
	e.Details = map[string]any{"required_fields": fields}
	return e
}
func bindingID(o harvest.Object) string {
	switch v := o["id"].(type) {
	case json.Number:
		return string(v)
	case string:
		return v
	case int:
		return fmt.Sprint(v)
	case int64:
		return fmt.Sprint(v)
	default:
		return ""
	}
}
func bindingObject(o harvest.Object, key string) harvest.Object {
	v, _ := o[key].(map[string]any)
	return v
}
func validTimezone(v string) bool {
	if !safeIdentifier(v, 128) || v == "Local" || strings.Contains(v, "\\") || strings.Contains(v, "..") || strings.HasPrefix(v, "/") {
		return false
	}
	_, err := time.LoadLocation(v)
	return err == nil
}
func validateLinkInput(in LinkInput) error {
	if !validUUID(in.RequestID) {
		return failure("validation")
	}
	if in.ProjectID == "" {
		return required("project_id")
	}
	for _, id := range []string{in.ProjectID, in.TaskID, in.AccountID} {
		if id != "" && !identity.Valid(id) {
			return failure("validation")
		}
	}
	if in.IfRevision != "" {
		if _, ok := counter(in.IfRevision); !ok {
			return failure("validation")
		}
	}
	if in.Timezone != "" && !validTimezone(in.Timezone) {
		return failure("validation")
	}
	return nil
}
func assignedAttribution(ctx context.Context, in LinkInput, d LinkDependencies, saved *BindingSnapshot) (Attribution, error) {
	account := in.AccountID
	if account == "" && d.ResolveAccount != nil {
		var err error
		account, err = d.ResolveAccount(ctx)
		if err != nil {
			return Attribution{}, err
		}
	}
	if account == "" {
		return Attribution{}, required("account_id")
	}
	if !identity.Valid(account) {
		return Attribution{}, failure("validation")
	}
	if saved != nil && saved.Attribution.AccountID == account && saved.Attribution.ProjectID == in.ProjectID {
		if in.Timezone == "" {
			in.Timezone = saved.Attribution.Timezone
		}
	}
	if in.Timezone == "" {
		return Attribution{}, required("timezone")
	}
	if d.NewProvider == nil {
		return Attribution{}, failure("auth")
	}
	api, err := d.NewProvider(ctx, account)
	if err != nil {
		return Attribution{}, err
	}
	if api == nil {
		return Attribution{}, failure("auth")
	}
	accounts, err := api.Accounts(ctx)
	if err != nil {
		return Attribution{}, err
	}
	found := false
	for _, a := range accounts {
		if bindingID(a) == account && a["product"] == "harvest" {
			found = true
		}
	}
	if !found {
		return Attribution{}, failure("forbidden")
	}
	user, err := api.Get(ctx, "/users/me")
	if err != nil {
		return Attribution{}, err
	}
	uid := bindingID(user)
	if !identity.Valid(uid) {
		return Attribution{}, failure("response")
	}
	if active, ok := user["is_active"].(bool); !ok {
		return Attribution{}, failure("response")
	} else if !active {
		return Attribution{}, failure("forbidden")
	}
	assignments, err := api.List(ctx, "/users/me/project_assignments", url.Values{})
	if err != nil {
		return Attribution{}, err
	}
	var selected harvest.Object
	for _, a := range assignments {
		project := bindingObject(a, "project")
		pid := bindingID(project)
		if !identity.Valid(pid) {
			return Attribution{}, failure("response")
		}
		if pid == in.ProjectID {
			if selected != nil {
				return Attribution{}, failure("response")
			}
			selected = a
		}
	}
	if selected == nil || selected["is_active"] != true || bindingObject(selected, "project")["is_active"] == false {
		return Attribution{}, failure("validation")
	}
	tasks, ok := selected["task_assignments"].([]any)
	if !ok {
		return Attribution{}, failure("response")
	}
	activeTasks := map[string]bool{}
	for _, raw := range tasks {
		row, ok := raw.(map[string]any)
		if !ok {
			return Attribution{}, failure("response")
		}
		task := bindingObject(row, "task")
		tid := bindingID(task)
		if !identity.Valid(tid) {
			return Attribution{}, failure("response")
		}
		if row["is_active"] == true && task["is_active"] != false {
			activeTasks[tid] = true
		}
	}
	if in.TaskID == "" && saved != nil && saved.Attribution.AccountID == account && saved.Attribution.ProjectID == in.ProjectID && activeTasks[saved.Attribution.TaskID] {
		in.TaskID = saved.Attribution.TaskID
	}
	if in.TaskID == "" && len(activeTasks) == 1 {
		for id := range activeTasks {
			in.TaskID = id
		}
	}
	if in.TaskID == "" {
		return Attribution{}, required("task_id")
	}
	if !activeTasks[in.TaskID] {
		return Attribution{}, failure("validation")
	}
	return Attribution{AccountID: account, UserID: uid, ProjectID: in.ProjectID, TaskID: in.TaskID, Timezone: in.Timezone}, nil
}
