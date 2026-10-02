package auth

import (
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/identity"
	"os"
)

func HandleNative(ctx context.Context, r NativeRequest, lock *os.File, d HandlerDependencies) (NativeReply, error) {
	reply := NativeReply{Effects: unchanged()}
	fail := func(code string) (NativeReply, error) { reply.Code = code; return reply, nil }
	if e := ctx.Err(); e != nil {
		return reply, e
	}
	if r.Operation != "read" && r.Operation != "login" && r.Operation != "logout" && r.Operation != "account" {
		return fail("validation")
	}
	if r.Operation != "read" {
		if e := validateMutationOwner(lock, d.LockPath); e != nil {
			return fail("validation")
		}
		if r.ConfigPath == "" {
			return fail("validation")
		}
	}
	if d.SaveConfig == nil {
		d.SaveConfig = Save
	}
	if d.LoadConfig == nil {
		d.LoadConfig = Load
	}
	if (r.Operation == "login" || r.Operation == "account") && !identity.Valid(r.AccountID) {
		return fail("validation")
	}
	if r.Operation == "account" {
		e := d.SaveConfig(r.ConfigPath, Config{Account: r.AccountID})
		if e != nil {
			if replaced(e) {
				reply.Effects.Config = "unknown"
			}
			return fail("config")
		}
		reply.Effects.Config = "saved"
		return reply, nil
	}
	if d.Store == nil {
		return fail("keychain")
	}
	if r.Operation == "read" {
		t, e := d.Store.Get()
		if errors.Is(e, ErrNotFound) {
			return fail("not_found")
		}
		if e != nil {
			return fail("keychain")
		}
		t, e = validateToken(t)
		if e != nil {
			return fail("keychain")
		}
		reply.Token = []byte(t)
		return reply, nil
	}
	if r.Operation == "logout" {
		e := d.Store.Delete()
		if e != nil && !errors.Is(e, ErrNotFound) {
			if unknownNative(e) {
				reply.Effects.Credential = "unknown"
				return fail("credential_write_unknown")
			}
			return fail("keychain")
		}
		reply.Effects.Credential = "applied"
		if e = d.SaveConfig(r.ConfigPath, Config{}); e != nil {
			if replaced(e) {
				reply.Effects.Config = "unknown"
			}
			return fail("config")
		}
		reply.Effects.Config = "cleared"
		return reply, nil
	}
	token, e := validateToken(string(r.Token))
	if e != nil {
		return fail("validation")
	}
	old, e := d.LoadConfig(r.ConfigPath)
	if e != nil {
		return fail("config")
	}
	restore := func() {
		if d.SaveConfig(r.ConfigPath, old) != nil {
			reply.Effects.Config = "unknown"
		} else {
			reply.Effects.Config = "restored"
		}
	}
	if e = d.SaveConfig(r.ConfigPath, Config{Account: r.AccountID}); e != nil {
		if replaced(e) {
			restore()
		}
		return fail("config")
	}
	reply.Effects.Config = "saved"
	if e = d.Store.Set(token); e != nil {
		if unknownNative(e) {
			reply.Effects.Credential = "unknown"
			return fail("credential_write_unknown")
		}
		restore()
		if reply.Effects.Config == "unknown" {
			return fail("config")
		}
		return fail("keychain")
	}
	reply.Effects.Credential = "applied"
	return reply, nil
}
func replaced(e error) bool { var se *SaveError; return errors.As(e, &se) && se.Replaced }
func unknownNative(e error) bool {
	var ae *Error
	return !errors.As(e, &ae) || ae.Uncertain || ae.Effects.Credential != "unchanged"
}
