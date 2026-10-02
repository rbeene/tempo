// Package setup shares readiness and guided binding actions between CLI and UI.
package setup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"time"
)

type Step struct {
	Action         string   `json:"action"`
	State          string   `json:"state"`
	RequiredFields []string `json:"required_fields"`
	SafeMessage    string   `json:"safe_message"`
}
type Status struct {
	ContractVersion int    `json:"contract_version"`
	Steps           []Step `json:"steps"`
	Complete        bool   `json:"complete"`
}
type Diagnostic struct {
	Code        string  `json:"code"`
	Severity    string  `json:"severity"`
	SafeMessage string  `json:"safe_message"`
	Action      *string `json:"action"`
}
type Diagnostics struct {
	ContractVersion int          `json:"contract_version"`
	Items           []Diagnostic `json:"items"`
}
type Input struct{ Host, Scope, Path, AccountID string }
type Options struct {
	Auth     *auth.Service
	Activity *activity.Service
}
type Service struct{ options Options }

func New(o Options) *Service { return &Service{options: o} }
func bounded[T any](ctx context.Context, f func(context.Context) (T, error)) (T, error) {
	c, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return f(c)
}
func missing(field string) error {
	return &auth.Error{Code: "input_required", Message: "complete the required setup choice", RequiredFields: []string{field}}
}
func uuid() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
func (s *Service) deps() activity.LinkDependencies {
	return activity.LinkDependencies{ResolveAccount: func(context.Context) (string, error) { return s.options.Auth.EffectiveAccount("") }, NewProvider: s.options.Auth.Provider}
}
func (s *Service) Link(ctx context.Context, in activity.LinkInput, p terminal.Prompter) (activity.BindingResult, error) {
	empty := activity.BindingResult{}
	if in.RequestID == "" {
		in.RequestID = uuid()
	}
	commit := func(c context.Context) (activity.BindingResult, error) {
		return s.options.Activity.Link(c, in, s.deps())
	}
	if p == nil || in.ProjectID != "" && in.TaskID != "" && in.Timezone != "" {
		return bounded(ctx, commit)
	}
	location, e := bounded(ctx, func(c context.Context) (activity.Location, error) { return activity.DiscoverLocation(c, in.Path) })
	if e != nil {
		return empty, e
	}
	in.AccountID, e = s.options.Auth.EffectiveAccount(in.AccountID)
	if e != nil {
		return empty, e
	}
	if in.AccountID == "" {
		as, e := bounded(ctx, s.options.Auth.Accounts)
		if e != nil {
			return empty, e
		}
		in.AccountID, e = chooseAccount(ctx, p, as)
		if e != nil {
			return empty, e
		}
	}
	api, e := bounded(ctx, func(c context.Context) (harvest.Provider, error) { return s.options.Auth.Provider(c, in.AccountID) })
	if e != nil {
		return empty, e
	}
	catalog, e := bounded(ctx, func(c context.Context) (activity.AssignmentCatalog, error) {
		return activity.DiscoverAssignments(c, in.AccountID, api)
	})
	if e != nil {
		return empty, e
	}
	var saved *activity.Binding
	shown, e := s.options.Activity.ShowBinding(ctx, activity.ShowBindingInput{Path: in.Path})
	if e == nil && len(shown.Bindings) > 0 {
		b := shown.Bindings[0]
		saved = &b
	} else if e != nil {
		var ae *activity.Error
		if !errors.As(e, &ae) || ae.Code != "not_found" {
			return empty, e
		}
	}
	if in.ProjectID == "" {
		choices := []terminal.Choice{}
		for _, project := range catalog.Projects {
			choices = append(choices, terminal.Choice{ID: project.ID, Label: terminal.Sanitize(fmt.Sprintf("%s · %s · account %s · project %s", project.Name, project.ClientName, in.AccountID, project.ID))})
		}
		if len(choices) == 0 {
			return empty, &auth.Error{Code: "input_required", Message: "no active assigned projects are available", RequiredFields: []string{"project_id"}}
		}
		in.ProjectID, e = p.Choose(ctx, "Choose a Harvest project", choices)
		if e != nil {
			return empty, e
		}
	}
	var project *activity.AssignedProject
	for i := range catalog.Projects {
		if catalog.Projects[i].ID == in.ProjectID {
			project = &catalog.Projects[i]
			break
		}
	}
	if project == nil {
		return empty, &auth.Error{Code: "validation", Message: "project is not an active assignment"}
	}
	if saved != nil && saved.Attribution.AccountID == in.AccountID && saved.Attribution.ProjectID == in.ProjectID {
		if in.Timezone == "" {
			in.Timezone = saved.Attribution.Timezone
		}
		if in.TaskID == "" {
			for _, t := range project.Tasks {
				if t.ID == saved.Attribution.TaskID {
					in.TaskID = t.ID
				}
			}
		}
	}
	if in.TaskID == "" && len(project.Tasks) == 1 {
		in.TaskID = project.Tasks[0].ID
	}
	if in.TaskID == "" {
		choices := []terminal.Choice{}
		for _, task := range project.Tasks {
			choices = append(choices, terminal.Choice{ID: task.ID, Label: terminal.Sanitize(task.Name + " · task " + task.ID)})
		}
		if len(choices) == 0 {
			return empty, missing("task_id")
		}
		in.TaskID, e = p.Choose(ctx, "Choose a task", choices)
		if e != nil {
			return empty, e
		}
	}
	if in.Timezone == "" {
		in.Timezone, e = p.Text(ctx, "IANA timezone (for example UTC or America/New_York)", "")
		if e != nil {
			return empty, e
		}
	}
	scope := location.Kind + " " + location.Locator
	if location.Kind == "repository" {
		scope += " (all Git worktrees inherit this mapping)"
	}
	confirm := fmt.Sprintf("Link %s to account %s / project %s / task %s / %s?", scope, in.AccountID, in.ProjectID, in.TaskID, in.Timezone)
	exactSaved := saved != nil && saved.Kind == location.Kind && saved.Locator == location.Locator
	if exactSaved {
		confirm = "Update existing mapping: " + confirm
	}
	yes, e := p.Confirm(ctx, terminal.Sanitize(confirm))
	if e != nil {
		return empty, e
	}
	if !yes {
		return empty, &terminal.ExitError{Code: 0}
	}
	if exactSaved && in.IfRevision == "" {
		in.IfRevision = saved.Revision
	}
	return bounded(ctx, commit)
}
func chooseAccount(ctx context.Context, p terminal.Prompter, as []harvest.Object) (string, error) {
	choices := []terminal.Choice{}
	for _, a := range as {
		if a["product"] != "harvest" {
			continue
		}
		id := fmt.Sprint(a["id"])
		choices = append(choices, terminal.Choice{ID: id, Label: terminal.Sanitize(fmt.Sprint(a["name"]) + " · account " + id)})
	}
	if len(choices) == 1 {
		return choices[0].ID, nil
	}
	if len(choices) == 0 {
		return "", missing("account_id")
	}
	return p.Choose(ctx, "Choose a Harvest account", choices)
}
func readiness() Status {
	return Status{ContractVersion: 1, Steps: []Step{{"auth.status", "input_required", []string{}, "Verify credentials with auth status --check or connect securely in interactive setup."}, {"bindings.link", "input_required", []string{"project_id", "task_id", "timezone"}, "Link a project and task to this directory or repository."}, {"hooks.status", "unsupported", []string{}, "Hook installation is not available in this version; no delivered events verified."}, {"sync.status", "input_required", []string{}, "Automatic upload is not configured or verified; authentication alone does not enable sync."}}, Complete: false}
}
func (s *Service) Run(ctx context.Context, in Input, p terminal.Prompter) (Status, error) {
	result := readiness()
	if p == nil {
		if _, e := s.options.Auth.EffectiveAccount(in.AccountID); e != nil {
			return result, e
		}
		shown, e := s.options.Activity.ShowBinding(ctx, activity.ShowBindingInput{Path: in.Path})
		if e != nil {
			var ae *activity.Error
			if !errors.As(e, &ae) || ae.Code != "not_found" {
				return result, e
			}
		}
		if len(shown.Bindings) > 0 {
			result.Steps[1] = Step{"bindings.link", "complete", []string{}, "Local mapping exists; remote assignment access has not been checked."}
		}
		return result, nil
	}
	loginCommitted := false
	status, e := bounded(ctx, func(c context.Context) (auth.Result, error) { return s.options.Auth.Status(c, true, in.AccountID) })
	if e != nil && !s.options.Auth.CanPersist() {
		result.Steps[0] = Step{"auth.login", "unsupported", []string{}, "Set HARVEST_TOKEN locally; secure token storage is unavailable on this platform."}
		return result, nil
	}
	if errors.Is(e, auth.ErrNotFound) || e == nil && !status.Authenticated {
		if !s.options.Auth.CanPersist() {
			result.Steps[0] = Step{"auth.login", "unsupported", []string{}, "Set HARVEST_TOKEN locally; secure token storage is unavailable on this platform."}
			return result, nil
		}
		token, e := p.Secret(ctx, "Harvest personal access token (hidden)")
		if e != nil {
			return result, e
		}
		attempt, e := bounded(ctx, func(c context.Context) (*auth.LoginAttempt, error) { return s.options.Auth.PrepareLogin(c, token) })
		clear(token)
		if e != nil {
			return result, e
		}
		defer attempt.Close()
		account, e := s.options.Auth.EffectiveAccount(in.AccountID)
		if e != nil {
			return result, e
		}
		if account == "" {
			account, e = chooseAccount(ctx, p, attempt.Accounts)
			if e != nil {
				return result, e
			}
		}
		yes, e := p.Confirm(ctx, "Save this credential securely for account "+terminal.Sanitize(account)+"?")
		if e != nil {
			return result, e
		}
		if !yes {
			return result, &terminal.ExitError{Code: 0}
		}
		status, e = bounded(ctx, func(c context.Context) (auth.Result, error) { return s.options.Auth.CommitLogin(c, attempt, account) })
		if e != nil {
			return result, e
		}
		in.AccountID = account
		loginCommitted = true
	} else if e != nil {
		return result, e
	}
	action := "auth.status"
	if loginCommitted {
		action = "auth.login"
	}
	result.Steps[0] = Step{action, "complete", []string{}, "Credential and accessible account verified; no hook or upload readiness inferred."}
	if in.Path == "" {
		in.Path, e = p.Text(ctx, "Directory to link (Enter uses current directory)", "")
		if e != nil {
			return result, e
		}
	}
	_, e = s.Link(ctx, activity.LinkInput{Path: in.Path, AccountID: in.AccountID}, p)
	if e != nil {
		var ee *terminal.ExitError
		if errors.As(e, &ee) && ee.Code == 0 {
			result.Steps[1].SafeMessage = "Link cancelled; any completed authentication step remains applied."
			return result, nil
		}
		return result, e
	}
	result.Steps[1] = Step{"bindings.link", "complete", []string{}, "Directory/repository mapping saved. Hook installation is the next setup step when available."}
	return result, nil
}
func (s *Service) Doctor(ctx context.Context, check bool) (Diagnostics, error) {
	action := "auth.status"
	result := Diagnostics{ContractVersion: 1, Items: []Diagnostic{{"credential_unverified", "info", "Credential validity has not been checked; use doctor --check.", &action}, {"hooks_unverified", "warning", "No installed hook or delivered event has been verified.", nil}, {"sync_unconfigured", "warning", "Upload configuration and background delivery are not verified.", nil}}}
	account, e := s.options.Auth.EffectiveAccount("")
	if e != nil {
		result.Items = append(result.Items, Diagnostic{"config_invalid", "error", "Local account configuration is invalid or unreadable.", nil})
	} else if account == "" {
		result.Items = append(result.Items, Diagnostic{"account_missing", "warning", "No account is selected.", &action})
	} else {
		result.Items = append(result.Items, Diagnostic{"account_configured", "info", "A local or environment account is selected; access is unverified.", &action})
	}
	bindings, e := s.options.Activity.ListBindings(ctx)
	if e != nil {
		result.Items = append(result.Items, Diagnostic{"state_invalid", "error", "Local activity state is invalid or unreadable; preserve it for inspection.", nil})
	} else if len(bindings.Bindings) == 0 {
		result.Items = append(result.Items, Diagnostic{"bindings_missing", "warning", "No local project mapping exists.", nil})
	} else {
		result.Items = append(result.Items, Diagnostic{"bindings_present", "info", "Local project mappings exist; assignment access is unverified.", nil})
	}
	if check {
		r, e := bounded(ctx, func(c context.Context) (auth.Result, error) { return s.options.Auth.Status(c, true, "") })
		if e != nil {
			return result, e
		}
		if r.Authenticated && r.Verified {
			result.Items[0] = Diagnostic{"credential_verified", "info", "Credential and account access verified.", &action}
		}
	}
	return result, nil
}
