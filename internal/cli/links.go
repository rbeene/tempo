package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
)

func linkCommand(name string) bool { return name == "link" || strings.HasPrefix(name, "links ") }
func linkRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("secure randomness unavailable")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
func executeLinks(ctx context.Context, p parsed, d Dependencies) (any, error) {
	service := activityService(d)
	f := p.flags
	id := ""
	if len(p.args) > 0 {
		id = p.args[0]
	}
	request := f["request-id"]
	if request == "" && p.command.Mutation {
		request = linkRequestID()
	}
	switch p.command.Name {
	case "links list":
		return service.ListBindings(ctx)
	case "links show":
		return service.ShowBinding(ctx, activity.ShowBindingInput{BindingID: id, Path: f["path"]})
	case "links unlink":
		return service.Unlink(ctx, activity.UnlinkInput{BindingID: id, IfRevision: f["if-revision"], RequestID: request, Confirmed: f["yes"] == "true"})
	case "links repair":
		return service.RepairBinding(ctx, activity.RepairBindingInput{BindingID: id, Path: f["path"], IfRevision: f["if-revision"], RequestID: request, Confirmed: f["yes"] == "true"})
	}
	deps := activity.LinkDependencies{
		ResolveAccount: func(context.Context) (string, error) {
			if account := d.Getenv("HARVEST_ACCOUNT_ID"); account != "" {
				return account, nil
			}
			path := d.ConfigPath
			if path == "" {
				path = d.Getenv("TEMPO_CONFIG")
			}
			if path == "" {
				var err error
				path, err = auth.ConfigPath()
				if err != nil {
					return "", problem("config", "cannot locate configuration")
				}
			}
			cfg, err := auth.Load(path)
			if err != nil {
				return "", problem("config", "cannot read account configuration")
			}
			return cfg.Account, nil
		},
		NewProvider: func(_ context.Context, account string) (harvest.Provider, error) {
			store := d.Store
			if store == nil {
				store = auth.NewStore()
			}
			token, _, err := auth.ResolveToken(store, d.Getenv)
			if err != nil {
				if errors.Is(err, auth.ErrNotFound) {
					return nil, problem("auth", "no token configured; use auth login --token-stdin or HARVEST_TOKEN")
				}
				return nil, problem("keychain", "cannot access token; unlock Keychain or use HARVEST_TOKEN")
			}
			if d.NewProvider != nil {
				return d.NewProvider(token, account), nil
			}
			return harvest.New(token, account), nil
		},
	}
	return service.Link(ctx, activity.LinkInput{ProjectID: id, TaskID: f["task"], Path: f["path"], AccountID: f["account"], Timezone: f["timezone"], IfRevision: f["if-revision"], RequestID: request}, deps)
}
