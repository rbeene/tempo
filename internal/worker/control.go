package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/rbeene/tempo/internal/privatefs"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func digest(b []byte) string { v := sha256.Sum256(b); return hex.EncodeToString(v[:]) }
func (s *Service) control(ctx context.Context, action string, r ControlRequest) (Result, error) {
	if !uuidPattern.MatchString(r.RequestID) {
		return Result{}, issue("validation")
	}
	if (action == "install" || action == "uninstall") && !r.Confirmed {
		return Result{}, issue("confirmation_required")
	}
	if s.options.Platform != "darwin" && s.options.Platform != "linux" {
		return Result{}, issue("unsupported")
	}
	if ctx.Err() != nil {
		return Result{}, ctx.Err()
	}
	expected := s.definition()
	if !validDefinition(expected) {
		return Result{}, issue("validation")
	}
	h, e := acquire(ctx, s.options.StatePath+".worker-control.json")
	if e != nil {
		return Result{}, e
	}
	defer h.close()
	record, raw, e := loadControl(h.root, h.name)
	if e != nil {
		return Result{}, e
	}
	in := controlIntent{Action: action, Request: r, Expected: expected}
	fingerprint, _ := json.Marshal(struct {
		Action     string
		Confirmed  bool
		Definition definition
	}{action, r.Confirmed, expected})
	in.Fingerprint = digest(fingerprint)
	if prior, ok := record.Receipts[r.RequestID]; ok {
		if prior.Intent.Fingerprint != in.Fingerprint {
			return Result{}, issue("request_conflict")
		}
		if e = syncExisting(h); e != nil {
			return Result{}, e
		}
		if prior.Error != nil {
			return Result{}, prior.Error
		}
		return *prior.Result, nil
	}
	if record.Pending != nil {
		if record.Pending.Request.RequestID != r.RequestID {
			return Result{}, issue("state_busy")
		}
		if record.Pending.Fingerprint != in.Fingerprint {
			return Result{}, issue("request_conflict")
		}
	}
	publishedPending := false
	removedPending := false
	if record.Owned != nil {
		if !sameDefinition(*record.Owned, expected) {
			return Result{}, issue("revision_conflict")
		}
		if e = s.verifyDefinition(*record.Owned); e != nil {
			if record.Pending == nil || action != "uninstall" || !sameDefinition(record.Pending.Expected, expected) || s.requireUnpublished(expected) != nil {
				return Result{}, e
			}
			removedPending = true
		}
	} else if e = s.requireUnpublished(expected); e != nil {
		if record.Pending == nil || action != "install" || !sameDefinition(record.Pending.Expected, expected) || s.verifyDefinition(expected) != nil {
			return Result{}, e
		}
		publishedPending = true
	}
	// A foreground owner can be stopped without installing a service.
	if action != "install" && action != "stop" && record.Owned == nil {
		return Result{}, issue("revision_conflict")
	}
	save := func() error {
		b, e := json.Marshal(record)
		if e != nil || len(b) > maxControlBytes {
			return issue("control_history_full")
		}
		verify := func() error {
			if e := h.verify(); e != nil {
				return e
			}
			current, e := readPrivate(h.root, h.name, maxControlBytes)
			if errors.Is(e, os.ErrNotExist) && raw == nil {
				return nil
			}
			if e != nil || !bytes.Equal(current, raw) {
				return issue("state_corrupt")
			}
			return nil
		}
		if e = atomicWrite(h.root, h.name, b, verify, s.fault); e != nil {
			return e
		}
		raw = b
		return nil
	}
	if record.Pending == nil {
		if len(record.Receipts) >= maxReceipts {
			return Result{}, issue("control_history_full")
		}
		record.Pending = &in
		b, _ := json.Marshal(record)
		if len(b)+len(fingerprint)+8192 > maxControlBytes {
			return Result{}, issue("control_history_full")
		}
		if e = save(); e != nil {
			return Result{}, e
		}
		if e = s.fault("control_after_reserved"); e != nil {
			return Result{}, e
		}
	}
	if action == "install" && record.Owned == nil {
		if publishedPending {
			e = s.recoverPublication(ctx, expected)
		} else {
			e = s.installDefinition(ctx, expected)
		}
		if e != nil {
			return Result{}, e
		}
		record.Owned = &expected
	}
	if action != "install" {
		if action == "stop" && record.Owned == nil {
			stopCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
			e = s.stopInstance(stopCtx)
			cancel()
		} else if removedPending {
			e = s.recoverRemoval(ctx, expected)
		} else {
			e = s.lifecycle(ctx, action, expected)
		}
		if e != nil {
			return Result{}, e
		}
		if e = s.fault("control_after_manager_effect"); e != nil {
			return Result{}, e
		}
		if action == "uninstall" {
			record.Owned = nil
		}
	}
	// Predict only completion of this controller's own pending intent; preserve
	// runtime diagnostics and all ordinary exact-definition/liveness checks.
	record.Pending = nil
	result, e := s.statusWithControl(ctx, &record)
	if e != nil {
		return Result{}, e
	}
	if e = s.fault("control_before_complete"); e != nil {
		return Result{}, e
	}
	record.Receipts[r.RequestID] = controlReceipt{Intent: in, Result: &result}
	record.Pending = nil
	if e = save(); e != nil {
		return Result{}, e
	}
	return result, nil
}
func sameDefinition(a, b definition) bool {
	return a.ServiceID == b.ServiceID && a.Path == b.Path && a.Digest == b.Digest && bytes.Equal(a.Bytes, b.Bytes)
}
func loadControl(root *os.Root, name string) (controlRecord, []byte, error) {
	b, e := readPrivate(root, name, maxControlBytes)
	if errors.Is(e, os.ErrNotExist) {
		return controlRecord{Version: 1, Receipts: map[string]controlReceipt{}}, nil, nil
	}
	var r controlRecord
	if e != nil {
		return r, nil, issue("state_corrupt")
	}
	if decode(b, &r) != nil || !validControl(r) {
		return r, nil, issue("state_corrupt")
	}
	return r, b, nil
}
func validDefinition(d definition) bool {
	return validPath(d.Path) && len(d.Bytes) > 0 && len(d.Bytes) < 65536 && d.Digest == digest(d.Bytes) && regexp.MustCompile(`^(io\.beene\.tempo\.worker\.[0-9a-f]{64}|tempo-worker-[0-9a-f]{64}\.service)$`).MatchString(d.ServiceID)
}
func validControl(r controlRecord) bool {
	if r.Version != 1 || r.Receipts == nil || len(r.Receipts) > maxReceipts {
		return false
	}
	if r.Owned != nil && !validDefinition(*r.Owned) {
		return false
	}
	validIntent := func(i controlIntent) bool {
		return uuidPattern.MatchString(i.Request.RequestID) && validDefinition(i.Expected) && len(i.Fingerprint) == 64 && (i.Action == "install" || i.Action == "start" || i.Action == "stop" || i.Action == "uninstall")
	}
	if r.Pending != nil && !validIntent(*r.Pending) {
		return false
	}
	for id, v := range r.Receipts {
		if id != v.Intent.Request.RequestID || !validIntent(v.Intent) || (v.Result == nil) == (v.Error == nil) {
			return false
		}
		if v.Result != nil && v.Result.ContractVersion != 1 {
			return false
		}
	}
	return true
}
func (s *Service) requireUnpublished(d definition) error {
	root, _, e := privatefs.OpenServiceDirectory(s.options.ServiceDir, false)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return fileError(e)
	}
	defer root.Close()
	_, e = root.Lstat(filepath.Base(d.Path))
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	return issue("revision_conflict")
}
func (s *Service) verifyDefinition(d definition) error {
	root, _, e := privatefs.OpenServiceDirectory(s.options.ServiceDir, false)
	if e != nil {
		return issue("revision_conflict")
	}
	defer root.Close()
	b, e := readPrivate(root, filepath.Base(d.Path), 65536)
	if e != nil || !bytes.Equal(b, d.Bytes) {
		return issue("revision_conflict")
	}
	return nil
}
func (s *Service) manager(ctx context.Context, args ...string) (CommandResult, error) {
	if s.options.Runner == nil {
		return CommandResult{}, issue("unsupported")
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	mutation := false
	if len(args) > 0 {
		switch args[0] {
		case "enable", "disable", "bootstrap", "bootout", "kickstart", "daemon-reload", "start", "stop":
			mutation = true
		}
	}
	executable := "/bin/launchctl"
	if s.options.Platform == "linux" {
		executable = "/usr/bin/systemctl"
		args = append([]string{"--user"}, args...)
	}
	r, e := s.options.Runner.Run(bounded, Command{Executable: executable, Args: args})
	if e != nil || len(r.Stdout)+len(r.Stderr) > 65536 || r.ExitCode != 0 {
		// A failed command may already have applied its local manager effect.
		// Keep the durable intent pending and require exact-request recovery.
		if mutation {
			return CommandResult{}, issue("local_write_unknown")
		}
		return CommandResult{}, issue("manager")
	}
	return r, nil
}
