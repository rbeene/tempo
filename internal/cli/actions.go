package cli

import (
	"encoding/json"
	"net/url"
	"strings"

	"github.com/rbeene/tempo/internal/harvest"
)

func (s *session) dispatch() (any, error) {
	n := s.p.command.Name
	f := s.p.flags
	switch n {
	case "projects list", "projects show", "tasks list", "clients list":
		return s.discover()
	case "time list":
		user, e := s.me()
		if e != nil {
			return nil, e
		}
		q := url.Values{"user_id": {user}}
		for k, v := range f {
			switch k {
			case "from", "to":
				q.Set(k, v)
			case "project", "task", "client":
				q.Set(k+"_id", v)
			case "running":
				q.Set("is_running", v)
			}
		}
		return s.api.List(s.ctx, "/time_entries", q)
	case "time show":
		return s.own(s.p.args[0])
	case "time create", "time update":
		body := s.entryBody()
		if n == "time update" {
			if _, e := s.own(s.p.args[0]); e != nil {
				return nil, e
			}
		}
		if _, h := body["hours"]; h || body["started_time"] != nil || n == "time create" {
			if e := s.checkMode(body, false); e != nil {
				return nil, e
			}
		}
		if n == "time create" {
			return s.api.Create(s.ctx, "/time_entries", body)
		}
		return s.api.Update(s.ctx, "/time_entries/"+s.p.args[0], body)
	case "time delete":
		id := s.p.args[0]
		if _, e := s.own(id); e != nil {
			return nil, e
		}
		if e := s.api.Delete(s.ctx, "/time_entries/"+id); e != nil {
			return nil, e
		}
		return map[string]any{"id": json.Number(id), "deleted": true}, nil
	case "timer status":
		return s.running()
	case "timer start":
		return s.start()
	case "timer stop":
		return s.stop()
	}
	return nil, problem("usage", "unknown command")
}
func (s *session) entryBody() harvest.Object {
	f := s.p.flags
	b := harvest.Object{}
	for _, k := range []string{"project", "task"} {
		if v, ok := f[k]; ok {
			b[k+"_id"] = json.Number(v)
		}
	}
	if v, ok := f["date"]; ok {
		b["spent_date"] = v
	}
	if v, ok := f["notes"]; ok {
		b["notes"] = v
	}
	for _, k := range []string{"hours", "duration"} {
		if v, ok := f[k]; ok {
			h, _ := hours(v)
			b["hours"] = h
		}
	}
	if v, ok := f["start"]; ok {
		b["started_time"], _ = clockTime(v)
	}
	if v, ok := f["end"]; ok {
		b["ended_time"], _ = clockTime(v)
	}
	return b
}
func (s *session) checkMode(body harvest.Object, timer bool) error {
	c, e := s.api.Get(s.ctx, "/company")
	if e != nil {
		return e
	}
	timestamp, ok := c["wants_timestamp_timers"].(bool)
	if !ok {
		return problem("response", "Harvest did not return company timer mode")
	}
	_, hasHours := body["hours"]
	_, hasStart := body["started_time"]
	if timestamp && hasHours {
		return problem("validation", "account uses start/end times; use --start and --end")
	}
	if !timestamp && hasStart {
		return problem("validation", "account uses duration; use --duration or --hours")
	}
	if timestamp && !timer && (!hasStart || body["ended_time"] == nil) {
		return problem("validation", "account uses start/end times; supply both --start and --end")
	}
	return nil
}
func (s *session) running() ([]harvest.Object, error) {
	user, e := s.me()
	if e != nil {
		return nil, e
	}
	entries, e := s.api.List(s.ctx, "/time_entries", url.Values{"user_id": {user}, "is_running": {"true"}})
	if e != nil {
		return nil, e
	}
	// Do not act on malformed or out-of-scope data, even if upstream filtered it.
	for _, o := range entries {
		if idOf(nested(o, "user")) != user || o["is_running"] != true || !validID(idOf(o)) {
			return nil, problem("response", "Harvest returned an invalid running timer")
		}
	}
	if entries == nil {
		entries = []harvest.Object{}
	}
	return entries, nil
}
func (s *session) start() (any, error) {
	var target harvest.Object
	if len(s.p.args) == 1 {
		var e error
		target, e = s.own(s.p.args[0])
		if e != nil {
			return nil, e
		}
	}
	active, e := s.running()
	if e != nil {
		return nil, e
	}
	if len(active) > 0 {
		if len(active) == 1 && target != nil && idOf(active[0]) == idOf(target) {
			return active[0], nil
		}
		return nil, problem("conflict", "a different timer is running; inspect timer status and stop it explicitly first")
	}
	if target != nil {
		running, stateErr := entryRunning(target)
		if stateErr != nil {
			return nil, stateErr
		}
		if running {
			return nil, problem("conflict", "timer state changed during preflight; inspect timer status")
		}
		return s.api.Update(s.ctx, "/time_entries/"+s.p.args[0]+"/restart", harvest.Object{})
	}
	body := s.entryBody()
	if e = s.checkMode(body, true); e != nil {
		return nil, e
	}
	return s.api.Create(s.ctx, "/time_entries", body)
}
func (s *session) stop() (any, error) {
	if len(s.p.args) == 1 {
		id := s.p.args[0]
		o, e := s.own(id)
		if e != nil {
			return nil, e
		}
		running, stateErr := entryRunning(o)
		if stateErr != nil {
			return nil, stateErr
		}
		if !running {
			return o, nil
		}
		return s.api.Update(s.ctx, "/time_entries/"+id+"/stop", harvest.Object{})
	}
	active, e := s.running()
	if e != nil {
		return nil, e
	}
	if len(active) == 0 {
		return map[string]any{"stopped": false}, nil
	}
	if len(active) > 1 {
		return nil, problem("conflict", "multiple timers are running; choose an explicit ID to stop")
	}
	return s.api.Update(s.ctx, "/time_entries/"+idOf(active[0])+"/stop", harvest.Object{})
}
func (s *session) discover() (any, error) {
	n := s.p.command.Name
	kind := strings.Split(n, " ")[0]
	if s.p.flags["all"] == "true" {
		return s.api.List(s.ctx, "/"+kind, url.Values{})
	}
	assignments, e := s.api.List(s.ctx, "/users/me/project_assignments", url.Values{})
	if e != nil {
		return nil, e
	}
	result := []harvest.Object{}
	seen := map[string]bool{}
	for _, a := range assignments {
		project := nested(a, "project")
		pid := idOf(project)
		if kind == "projects" {
			if n == "projects show" {
				if pid == s.p.args[0] {
					return a, nil
				}
				continue
			}
			result = append(result, a)
			continue
		}
		if kind == "clients" {
			client := nested(a, "client")
			id := idOf(client)
			if validID(id) && !seen[id] {
				seen[id] = true
				result = append(result, client)
			}
			continue
		}
		if filter := s.p.flags["project"]; filter != "" && pid != filter {
			continue
		}
		// User project assignments include the tasks this user can track.
		tasks, ok := a["task_assignments"].([]any)
		if !ok {
			return nil, problem("response", "Harvest project assignment is missing task assignments")
		}
		for _, v := range tasks {
			ta, ok := v.(map[string]any)
			if !ok {
				return nil, problem("response", "Harvest returned an invalid task assignment")
			}
			result = append(result, harvest.Object{"project": project, "task_assignment": ta})
		}
	}
	if n == "projects show" {
		return nil, problem("not_found", "project is not assigned to the current user")
	}
	return result, nil
}

func entryRunning(o harvest.Object) (bool, error) {
	running, ok := o["is_running"].(bool)
	if !ok {
		return false, problem("response", "Harvest returned an invalid timer state")
	}
	return running, nil
}
