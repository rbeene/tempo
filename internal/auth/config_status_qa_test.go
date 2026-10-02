package auth

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQAConfigShowPreservesPrecedenceShapeAndNoCredentialReads(t *testing.T) {
	for _, tc := range []struct{ name, explicit, env, saved, want string }{{"saved", "", "", "11", "11"}, {"environment", "", "22", "11", "22"}, {"explicit", "33", "22", "11", "33"}, {"empty", "", "", "", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			before := []byte(`{"account_id":"` + tc.saved + `"}`)
			if e := os.WriteFile(path, before, 0600); e != nil {
				t.Fatal(e)
			}
			s := NewService(Options{ConfigPath: path, Getenv: func(k string) string {
				switch k {
				case "HARVEST_ACCOUNT_ID":
					return tc.env
				case "HARVEST_TOKEN":
					return "SYNTHETIC-SECRET"
				}
				return ""
			}, PersistentAvailable: func() bool { t.Error("config inspection queried credential availability"); return true }, Runner: RunnerFunc(func(context.Context, NativeRequest, *os.File) (NativeReply, error) {
				t.Error("config inspection read credential store")
				return NativeReply{}, errors.New("forbidden")
			})})
			got, e := s.ConfigShow(context.Background(), tc.explicit)
			if e != nil {
				t.Fatal(e)
			}
			if got.Path != path || got.SavedAccountID != tc.saved || got.AccountID != tc.want || got.TokenStoredInConfig {
				t.Errorf("wrong config result: %#v", got)
			}
			b, _ := json.Marshal(got)
			var shape map[string]any
			json.Unmarshal(b, &shape)
			if len(shape) != 4 {
				t.Errorf("wrong legacy shape: %s", b)
			}
			for _, k := range []string{"path", "saved_account_id", "account_id", "token_stored_in_config"} {
				if _, ok := shape[k]; !ok {
					t.Errorf("missing key %s", k)
				}
			}
			if strings.Contains(string(b), "SYNTHETIC-SECRET") {
				t.Error("secret leaked")
			}
			after, _ := os.ReadFile(path)
			if string(after) != string(before) {
				t.Error("read-only config changed bytes")
			}
		})
	}
}
func TestQAConfigShowUsesInjectedEnvironmentPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cfg")
	os.WriteFile(path, []byte(`{"account_id":"44"}`), 0600)
	s := NewService(Options{Getenv: func(k string) string {
		if k == "TEMPO_CONFIG" {
			return path
		}
		return ""
	}})
	r, e := s.ConfigShow(context.Background(), "")
	if e != nil || r.Path != path || r.AccountID != "44" {
		t.Fatalf("injected config path not used: %#v %v", r, e)
	}
}
func TestQAConfigShowInvalidConfigAndSelectionFailSafely(t *testing.T) {
	for _, tc := range []struct{ name, data, explicit, env, code string }{{"unknown-field", `{"account_id":"11","token":"SYNTHETIC-SECRET"}`, "22", "", "config"}, {"duplicate", `{"account_id":"11","account_id":"22"}`, "", "", "config"}, {"invalid-explicit", `{"account_id":"11"}`, "oops", "", "validation"}, {"invalid-env", `{"account_id":"11"}`, "", "oops", "validation"}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cfg")
			os.WriteFile(path, []byte(tc.data), 0600)
			s := NewService(Options{ConfigPath: path, Getenv: func(k string) string {
				if k == "HARVEST_ACCOUNT_ID" {
					return tc.env
				}
				return ""
			}})
			r, e := s.ConfigShow(context.Background(), tc.explicit)
			var ae *Error
			if !errors.As(e, &ae) || ae.Code != tc.code {
				t.Fatalf("unsafe config acceptance %#v %v", r, e)
			}
			if strings.Contains(e.Error(), "SYNTHETIC-SECRET") || strings.Contains(e.Error(), "oops") {
				t.Error("unsafe raw config diagnosis")
			}
			if r != (ConfigStatus{}) {
				t.Errorf("error returned partial config %#v", r)
			}
		})
	}
}
func TestQAConfigShowCanceledNeverReadsPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := NewService(Options{Getenv: func(string) string { t.Error("canceled inspection consulted environment"); return "" }})
	r, e := s.ConfigShow(ctx, "")
	if !errors.Is(e, context.Canceled) || r != (ConfigStatus{}) {
		t.Errorf("cancellation ignored: %#v %v", r, e)
	}
}
