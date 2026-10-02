package worker

import (
	"context"
	"strconv"
	"strings"
	"time"
)

type unitState struct{ loaded, active, enabled bool }

func (s *Service) inspectUnit(ctx context.Context, d definition) (unitState, error) {
	var state unitState
	version, e := s.manager(ctx, "--version")
	if e != nil {
		return state, e
	}
	line, _, _ := strings.Cut(string(version.Stdout), "\n")
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != "systemd" {
		return state, issue("unsupported")
	}
	n, e := strconv.Atoi(fields[1])
	if e != nil || n < 247 {
		return state, issue("unsupported")
	}
	r, e := s.manager(ctx, "show", "--property=LoadState,ActiveState,FragmentPath,UnitFileState", "--", d.ServiceID)
	if e != nil {
		return state, e
	}
	values := map[string]string{}
	if len(r.Stdout) == 0 || r.Stdout[len(r.Stdout)-1] != '\n' {
		return state, issue("manager")
	}
	for _, line := range strings.Split(strings.TrimSuffix(string(r.Stdout), "\n"), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return state, issue("manager")
		}
		if _, dup := values[k]; dup {
			return state, issue("manager")
		}
		values[k] = v
	}
	if len(values) != 4 {
		return state, issue("manager")
	}
	for _, key := range []string{"LoadState", "ActiveState", "FragmentPath", "UnitFileState"} {
		if _, ok := values[key]; !ok {
			return state, issue("manager")
		}
	}
	if values["LoadState"] == "not-found" && values["ActiveState"] == "inactive" && values["FragmentPath"] == "" && values["UnitFileState"] == "" {
		return state, nil
	}
	if values["LoadState"] != "loaded" || values["FragmentPath"] != d.Path {
		return state, issue("revision_conflict")
	}
	if values["ActiveState"] != "active" && values["ActiveState"] != "inactive" && values["ActiveState"] != "failed" {
		return state, issue("manager")
	}
	if values["UnitFileState"] != "enabled" && values["UnitFileState"] != "disabled" {
		return state, issue("manager")
	}
	state.loaded = true
	state.active = values["ActiveState"] == "active"
	state.enabled = values["UnitFileState"] == "enabled"
	return state, nil
}
func (s *Service) installLinux(ctx context.Context, d definition) error {
	state, e := s.inspectUnit(ctx, d)
	if e != nil {
		return e
	}
	if state.loaded {
		return issue("revision_conflict")
	}
	if e = s.publishDefinition(d); e != nil {
		return e
	}
	return s.finishLinuxPublication(ctx, d)
}
func (s *Service) recoverLinuxPublication(ctx context.Context, d definition) error {
	if e := s.resyncPublication(d); e != nil {
		return e
	}
	state, e := s.inspectUnit(ctx, d)
	if e != nil {
		return e
	}
	if state.active || state.enabled {
		return issue("revision_conflict")
	}
	return s.finishLinuxPublication(ctx, d)
}
func (s *Service) finishLinuxPublication(ctx context.Context, d definition) error {
	if _, e := s.manager(ctx, "daemon-reload"); e != nil {
		return e
	}
	state, e := s.inspectUnit(ctx, d)
	if e != nil {
		return e
	}
	if !state.loaded || state.enabled || state.active {
		return issue("manager")
	}
	return nil
}
func (s *Service) lifecycleLinux(ctx context.Context, action string, d definition) error {
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	state, e := s.inspectUnit(ctx, d)
	if e != nil {
		return e
	}
	if !state.loaded {
		return issue("revision_conflict")
	}
	if action == "start" {
		if _, e = s.manager(ctx, "enable", "--", d.ServiceID); e != nil {
			return e
		}
		if e = s.fault("control_after_manager_effect"); e != nil {
			return e
		}
		if !state.active {
			if _, e = s.manager(ctx, "start", "--", d.ServiceID); e != nil {
				return e
			}
		}
		state, e = s.inspectUnit(ctx, d)
		if e != nil {
			return e
		}
		if !state.enabled {
			return issue("manager")
		}
		return nil
	}
	if _, e = s.manager(ctx, "disable", "--", d.ServiceID); e != nil {
		return e
	}
	if e = s.fault("control_after_disabled"); e != nil {
		return e
	}
	state, e = s.inspectUnit(ctx, d)
	if e != nil {
		return e
	}
	if state.enabled {
		return issue("manager")
	}
	if _, e = s.manager(ctx, "stop", "--", d.ServiceID); e != nil {
		return e
	}
	state, e = s.inspectUnit(ctx, d)
	if e != nil {
		return e
	}
	if state.active || state.enabled {
		return issue("manager")
	}
	if e = s.stopInstance(ctx); e != nil {
		return e
	}
	if action == "uninstall" {
		if e = s.removeDefinition(d); e != nil {
			return e
		}
		if _, e = s.manager(ctx, "daemon-reload"); e != nil {
			return e
		}
	}
	return nil
}
