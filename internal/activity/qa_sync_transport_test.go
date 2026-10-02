package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
)

func TestQASyncCommittedHTTPResponseLossReconcilesWithoutSecondPost(t *testing.T) {
	for _, collision := range []bool{false, true} {
		t.Run(fmt.Sprintf("late-page-collision-%t", collision), func(t *testing.T) {
			s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
			var mu sync.Mutex
			posts, pages := 0, 0
			var committed harvest.Object
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.URL.Path != "/id/accounts" && r.Header.Get("Harvest-Account-Id") != "1" {
					t.Error("transport lost immutable account scope")
				}
				switch r.URL.Path {
				case "/id/accounts":
					fmt.Fprint(w, `{"accounts":[{"id":1,"product":"harvest"}]}`)
				case "/v2/users/me":
					fmt.Fprint(w, `{"id":2,"is_active":true,"timezone":"UTC"}`)
				case "/v2/company":
					fmt.Fprint(w, `{"is_active":true,"wants_timestamp_timers":false,"clock":"24h"}`)
				case "/v2/users/me/project_assignments":
					fmt.Fprint(w, `{"project_assignments":[{"is_active":true,"project":{"id":3},"task_assignments":[{"is_active":true,"task":{"id":4}}]}],"links":{"next":null}}`)
				case "/v2/time_entries":
					if r.Method == "POST" {
						posts++
						var body harvest.Object
						dec := json.NewDecoder(r.Body)
						dec.UseNumber()
						if err := dec.Decode(&body); err != nil {
							t.Error(err)
						}
						committed = qaSyncEntryFromPost(body)
						// Server committed the mutation, but intentionally loses every response byte.
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						conn.Close()
						return
					}
					if r.Method != "GET" {
						t.Errorf("unexpected method %s", r.Method)
						w.WriteHeader(500)
						return
					}
					pages++
					q := r.URL.Query()
					if q.Get("user_id") != "2" || q.Get("external_reference_id") == "" || q.Get("project_id") != "" || q.Get("task_id") != "" || q.Get("from") != "" || q.Get("to") != "" {
						t.Errorf("collision-hiding query %v", q)
					}
					if collision && q.Get("cursor") == "" {
						next := url.Values{"user_id": {"2"}, "external_reference_id": {q.Get("external_reference_id")}, "cursor": {"second"}}
						json.NewEncoder(w).Encode(harvest.Object{"time_entries": []harvest.Object{committed}, "links": harvest.Object{"next": "?" + next.Encode()}})
						return
					}
					entry := committed
					if collision {
						entry = qaSyncEntryFromPost(committed)
						entry["id"] = json.Number("902")
						entry["spent_date"] = "2026-09-01"
					}
					json.NewEncoder(w).Encode(harvest.Object{"time_entries": []harvest.Object{entry}, "links": harvest.Object{"next": nil}})
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			provider := harvest.NewWithHTTP("qa-placeholder", "1", server.URL+"/v2", server.URL+"/id", server.Client())
			deps := SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return provider, nil }}
			_, err := s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "nearest-hundredth-hour", IfRevision: "0", RequestID: qaSyncID(1), Confirmed: true}, deps)
			if err != nil {
				t.Fatal(err)
			}
			qaSyncEnable(t, s)
			_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(150)}, deps)
			if err != nil {
				t.Fatal(err)
			}
			item := qaSyncOnlyItem(t, s)
			if item.State != "unknown" {
				t.Fatalf("lost response classified %+v", item)
			}
			_, err = s.SyncReconcile(context.Background(), SyncReconcileInput{RequestID: qaSyncID(151), OutboxID: item.ID}, deps)
			if err != nil {
				t.Fatal(err)
			}
			item = qaSyncOnlyItem(t, s)
			if (item.State == "synced") == collision {
				t.Fatalf("full-list reconcile disposition=%+v", item)
			}
			_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(152)}, deps)
			if err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			wantPages := 1
			if collision {
				wantPages = 2
			}
			if posts != 1 || pages != wantPages {
				t.Fatalf("remote effects posts%d pages%d", posts, pages)
			}
		})
	}
}
