package cli

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"strings"

	"github.com/rbeene/tempo/internal/hookstate"
)

func hooksCommand(name string) bool { return strings.HasPrefix(name, "hooks ") }
func hooksService(d Dependencies) *hookstate.Service {
	if d.Hooks != nil {
		return d.Hooks
	}
	return hookstate.New(hookstate.Options{Path: d.Getenv("TEMPO_HOOK_STATE"), BuildVersion: Version})
}
func validateHooksCLI(p *parsed) error {
	f := p.flags
	if _, ok := f["account"]; ok {
		return problem("usage", "hooks are local operations; --account is not valid")
	}
	if f["host"] != "codex" && f["host"] != "claude" && f["host"] != "both" {
		return problem("validation", "--host must be codex, claude or both")
	}
	if f["scope"] != "user" && f["scope"] != "project" {
		return problem("validation", "--scope must be user or project")
	}
	if f["scope"] == "project" && f["path"] == "" {
		return problem("validation", "project scope requires --path")
	}
	if f["path"] != "" && !filepath.IsAbs(f["path"]) {
		return problem("validation", "--path must be absolute")
	}
	switch p.command.Name {
	case "hooks preview":
		if f["operation"] != "install" && f["operation"] != "repair" && f["operation"] != "uninstall" {
			return problem("validation", "preview requires --operation install, repair or uninstall")
		}
	case "hooks confirm-profile", "hooks revoke-profile":
		if f["host"] == "both" || f["path"] == "" {
			return problem("validation", "profile actions require a single host and explicit project --path")
		}
	}
	if p.command.Mutation {
		if p.command.Name != "hooks revoke-profile" {
			b, e := hex.DecodeString(f["fingerprint"])
			if e != nil || len(b) != 32 || hex.EncodeToString(b) != f["fingerprint"] {
				return problem("validation", "mutation requires the exact preview --fingerprint")
			}
		} else if f["if-revision"] == "" || f["if-revision"] == "0" {
			return problem("validation", "revoke requires a positive --if-revision")
		}
		if p.command.Name == "hooks confirm-profile" && f["declaration-version"] != hookstate.DeclarationVersion {
			return problem("validation", "review and provide the current --declaration-version")
		}
		if f["yes"] != "true" {
			return problem("confirmation_required", "review the preview and explicitly confirm with --yes")
		}
	} else if f["yes"] == "true" {
		return problem("usage", "--yes is not valid for a read-only hook action")
	}
	return nil
}
func executeHooks(ctx context.Context, p parsed, d Dependencies) (any, error) {
	s := hooksService(d)
	f := p.flags
	if p.command.Mutation && f["request-id"] == "" {
		f["request-id"] = linkRequestID()
	}
	selector := hookstate.HookSelector{Host: f["host"], Scope: f["scope"], Path: f["path"]}
	intent := hookstate.InstallIntent{Host: selector.Host, Scope: selector.Scope, Path: selector.Path, Operation: f["operation"]}
	switch p.command.Name {
	case "hooks preview":
		return s.PreviewInstall(ctx, intent)
	case "hooks status":
		return s.Status(ctx, selector)
	case "hooks verify":
		return s.Verify(ctx, selector)
	case "hooks install", "hooks repair", "hooks uninstall":
		intent.Operation = strings.TrimPrefix(p.command.Name, "hooks ")
		return s.ApplyInstall(ctx, hookstate.ApplyInstallInput{Intent: intent, Fingerprint: f["fingerprint"], RequestID: f["request-id"], Confirmed: f["yes"] == "true"})
	case "hooks confirm-profile":
		return s.ConfirmInstalled(ctx, hookstate.InstalledConfirmInput{Selector: selector, Fingerprint: f["fingerprint"], DeclarationVersion: f["declaration-version"], RequestID: f["request-id"], Confirmed: f["yes"] == "true"})
	case "hooks revoke-profile":
		profile, err := s.Revoke(ctx, hookstate.RevokeInput{Host: selector.Host, Scope: selector.Scope, Path: selector.Path, IfRevision: f["if-revision"], RequestID: f["request-id"], Confirmed: f["yes"] == "true"})
		if err != nil {
			return nil, err
		}
		return hookstate.ProfileResult(profile, f["request-id"]), nil
	}
	return nil, problem("usage", "unknown hooks action")
}
