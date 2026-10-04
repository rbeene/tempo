package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
)

type qaCLISyncScopeAPI struct {
	qaCLILinkAPI
	user string
}

func (p *qaCLISyncScopeAPI) Get(ctx context.Context, path string) (harvest.Object, error) {
	if path == "/users/me" {
		return harvest.Object{"id": p.user, "is_active": true}, nil
	}
	return p.qaCLILinkAPI.Get(ctx, path)
}
func qaSyncGuardCLIState(t *testing.T) (*activity.Service, string, *qaCLISyncScopeAPI) {
	t.Helper()
	root := t.TempDir()
	project := filepath.Join(root, "project")
	if e := os.Mkdir(project, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(root, "activity", "state.json")
	service := activity.New(activity.Options{Path: path})
	api := &qaCLISyncScopeAPI{user: "7"}
	_, e := service.Link(context.Background(), activity.LinkInput{Path: project, ProjectID: "100", TaskID: "200", AccountID: "11", Timezone: "UTC", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, activity.LinkDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return api, nil }})
	if e != nil {
		t.Fatalf("invalid synthetic binding fixture: %v", e)
	}
	return service, path, api
}
func qaSyncGuardInput(id string) activity.SyncConfigureInput {
	return activity.SyncConfigureInput{AccountID: "11", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: id, Confirmed: true}
}
func qaSyncGuardCLIArgs(id, revision string) []string {
	return []string{"sync", "configure", "--account", "11", "--mode", "duration", "--duration-policy", "exact", "--if-revision", revision, "--request-id", id, "--yes", "--json"}
}
func TestQASyncCLIExpectedUserRejectsOtherUserEqualRevisionWithoutWrites(t *testing.T) {
	service, path, api := qaSyncGuardCLIState(t)
	deps := activity.SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return api, nil }}
	for i, user := range []string{"7", "8"} {
		api.user = user
		in := qaSyncGuardInput([]string{"90000000-0000-4000-8000-000000000861", "90000000-0000-4000-8000-000000000862"}[i])
		in.UserID = user
		if _, e := service.SyncConfigure(context.Background(), in, deps); e != nil {
			t.Fatal(e)
		}
	}
	status, e := service.SyncStatus(context.Background())
	if e != nil || len(status.Configurations) != 2 {
		t.Fatal("invalid equal revision fixture")
	}
	for _, c := range status.Configurations {
		if c.Revision != "1" {
			t.Fatal("unequal setup revision")
		}
	}
	before := status
	credentialReads := 0
	credentials := auth.NewService(auth.Options{ConfigPath: filepath.Join(filepath.Dir(path), "missing-config"), Getenv: func(k string) string {
		if k == "HARVEST_TOKEN" {
			credentialReads++
			return "synthetic-cli-scope-token"
		}
		return ""
	}, NewProvider: func(_, account string) harvest.Provider {
		if account != "11" {
			t.Error("wrong explicit account")
		}
		return api
	}})
	d := cli.Dependencies{Activity: service, Auth: credentials, Getenv: func(string) string { return "" }}
	args := append(qaSyncGuardCLIArgs("90000000-0000-4000-8000-000000000863", "1"), "--user", "7")
	// identity_conflict preserves the existing shared CLI classification (exit1).
	v := qaSyncCLIEnvelope(t, args, d, 1)
	errObj, _ := v["error"].(map[string]any)
	if errObj["code"] != "identity_conflict" {
		t.Errorf("guard didn't verify current user: %v", v)
	}
	after, e := service.SyncStatus(context.Background())
	if e != nil || !reflect.DeepEqual(before, after) {
		t.Error("CLI wrong-user guard changed consent or receipt")
	}
	if credentialReads == 0 {
		t.Error("canonical guard was not forwarded to actual verified service")
	}
}
func TestQASyncCLIUserGuardValidationAndCommandScopeStayOffline(t *testing.T) {
	for _, user := range []string{"0", "01", "-1", "abc", " 7", "7\n"} {
		t.Run(user, func(t *testing.T) {
			d, path := qaSyncCLIDeps(t)
			args := append(qaSyncGuardCLIArgs("90000000-0000-4000-8000-000000000864", "0"), "--user", user)
			v := qaSyncCLIEnvelope(t, args, d, 2)
			errObj, _ := v["error"].(map[string]any)
			if errObj["code"] != "validation" {
				t.Errorf("malformed user not validated as canonical ID: %v", v)
			}
			if _, e := os.Stat(filepath.Dir(path)); !os.IsNotExist(e) {
				t.Error("malformed guard initialized store")
			}
		})
	}
	for _, command := range []string{"status", "now", "pause", "resume", "reconcile"} {
		t.Run(command, func(t *testing.T) {
			d, _ := qaSyncCLIDeps(t)
			v := qaSyncCLIEnvelope(t, []string{"sync", command, "--user", "7", "--json"}, d, 2)
			errObj, _ := v["error"].(map[string]any)
			if errObj["code"] != "usage" {
				t.Error("configure-only guard accepted elsewhere")
			}
		})
	}
}
func TestQASyncCLILegacyAndGuardedOfflineReceiptReplay(t *testing.T) {
	for _, guard := range []bool{false, true} {
		name := "legacy"
		if guard {
			name = "guarded"
		}
		t.Run(name, func(t *testing.T) {
			service, path, api := qaSyncGuardCLIState(t)
			in := qaSyncGuardInput("90000000-0000-4000-8000-000000000865")
			if guard {
				in.UserID = "7"
			}
			result, e := service.SyncConfigure(context.Background(), in, activity.SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return api, nil }})
			if e != nil {
				t.Fatal(e)
			}
			d, _ := qaSyncCLIDeps(t)
			d.Activity = activity.New(activity.Options{Path: path})
			args := qaSyncGuardCLIArgs(in.RequestID, "0")
			if guard {
				args = append(args, "--user", "7")
			}
			v := qaSyncCLIEnvelope(t, args, d, 0)
			data, _ := v["data"].(map[string]any)
			saved, _ := json.Marshal(result)
			reported, _ := json.Marshal(data)
			var expected map[string]any
			json.Unmarshal(saved, &expected)
			normalized, _ := json.Marshal(expected)
			if !bytes.Equal(normalized, reported) {
				t.Errorf("offline replay changed exact receipt: %s/%s", normalized, reported)
			}
			if guard {
				changed := qaSyncGuardCLIArgs(in.RequestID, "0")
				changed = append(changed, "--user", "8")
				v = qaSyncCLIEnvelope(t, changed, d, 6)
				errObj, _ := v["error"].(map[string]any)
				if errObj["code"] != "request_conflict" {
					t.Error("changed guard did not conflict before credentials")
				}
			}
		})
	}
}
func TestQASyncCLIUserGuardOfflineSchema(t *testing.T) {
	d, _ := qaSyncCLIDeps(t)
	v := qaSyncCLIEnvelope(t, []string{"schema", "--json"}, d, 0)
	data, _ := v["data"].(map[string]any)
	commands, _ := data["commands"].([]any)
	found := false
	for _, item := range commands {
		command, _ := item.(map[string]any)
		name, _ := command["name"].(string)
		flags, _ := command["flags"].(map[string]any)
		if name == "sync configure" {
			found = true
			if flags["user"] != "id" {
				t.Errorf("configure schema hides canonical user guard: %v", flags)
			}
		} else if strings.HasPrefix(name, "sync ") {
			if _, ok := flags["user"]; ok {
				t.Errorf("schema broadened user guard to %s", name)
			}
		}
	}
	if !found {
		t.Fatal("missing offline configure schema")
	}
}
