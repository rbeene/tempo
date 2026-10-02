package activity

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rbeene/tempo/internal/harvest"
)

func TestQABindingValidationUsesSelectedAccountAndCompletePagination(t *testing.T) {
	for _, brokenSecondPage := range []bool{false, true} {
		t.Run(fmt.Sprintf("broken-second-page-%t", brokenSecondPage), func(t *testing.T) {
			pages := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("remote mutation: %s", r.Method)
					w.WriteHeader(500)
					return
				}
				switch r.URL.Path {
				case "/id/accounts":
					fmt.Fprint(w, `{"accounts":[{"id":1,"product":"harvest"},{"id":9,"product":"harvest"}]}`)
				case "/v2/users/me":
					if r.Header.Get("Harvest-Account-Id") != "9" {
						t.Error("user fetched in wrong account")
					}
					fmt.Fprint(w, `{"id":92,"is_active":true}`)
				case "/v2/users/me/project_assignments":
					if r.Header.Get("Harvest-Account-Id") != "9" {
						t.Error("assignments fetched in wrong account")
					}
					pages++
					if r.URL.Query().Get("cursor") == "second" {
						if brokenSecondPage {
							fmt.Fprint(w, `{"project_assignments":null,"links":{"next":null}}`)
						} else {
							fmt.Fprint(w, `{"project_assignments":[{"is_active":true,"project":{"id":3},"task_assignments":[{"is_active":true,"task":{"id":4}}]}],"links":{"next":null}}`)
						}
					} else {
						fmt.Fprint(w, `{"project_assignments":[{"is_active":true,"project":{"id":8},"task_assignments":[]}],"links":{"next":"?cursor=second"}}`)
					}
				default:
					t.Errorf("unexpected endpoint %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			s, path := qaLinkService(t)
			in := qaLinkInput(t)
			in.AccountID = ""
			d := LinkDependencies{ResolveAccount: func(context.Context) (string, error) { return "9", nil }, NewProvider: func(_ context.Context, account string) (harvest.Provider, error) {
				if account != "9" {
					t.Fatalf("selected account not supplied: %s", account)
				}
				return harvest.NewWithHTTP("synthetic-only", account, server.URL+"/v2", server.URL+"/id", server.Client()), nil
			}}
			r, err := s.Link(context.Background(), in, d)
			if brokenSecondPage {
				if err == nil {
					t.Fatal("accepted incomplete assignments")
				}
				qaAbsentLinkState(t, path)
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if r.Binding.Attribution.AccountID != "9" || r.Binding.Attribution.UserID != "92" || r.Binding.Attribution.ProjectID != "3" {
					t.Fatalf("cross-account attribution: %+v", r)
				}
			}
			if pages != 2 {
				t.Fatalf("assignment pagination incomplete: %d", pages)
			}
		})
	}
}
