package harvest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"
)

func TestQASyncHTTPCreateRequiresExactly201AndNeverRetries(t *testing.T) {
	for _, status := range []int{200, 201, 202, 204, 206, 207, 301, 302, 307, 308, 400, 401, 403, 404, 408, 422, 429, 500, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			calls := 0
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/v2/time_entries" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Location", "/v2/forbidden-followup")
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(status)
				fmt.Fprint(w, `{"id":901,"hours":0.040,"is_running":false}`)
			})
			got, err := c.Create(context.Background(), "/time_entries", Object{"project_id": json.Number("3"), "task_id": json.Number("4"), "spent_date": "2026-10-02", "hours": json.Number("0.04")})
			if calls != 1 {
				t.Fatalf("write attempts=%d", calls)
			}
			if status == 201 {
				if err != nil || got["id"] != json.Number("901") {
					t.Fatalf("201=%v err=%v", got, err)
				}
				return
			}
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("status%d was accepted: got=%v err=%v", status, got, err)
			}
			conclusive := status == 400 || status == 401 || status == 403 || status == 404 || status == 422 || status == 429
			if e.Uncertain == conclusive || e.Retryable {
				t.Fatalf("status%d uncertainty/retry=%+v", status, e)
			}
		})
	}
}
func TestQASyncHTTPListPreservesCorrelationScopeAcrossAllPages(t *testing.T) {
	calls := 0
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if q.Get("user_id") != "2" || q.Get("external_reference_id") != "tempo:v1:part" || len(q) > 3 {
			t.Errorf("reconcile scope=%v", q)
		}
		if calls == 1 {
			fmt.Fprint(w, `{"time_entries":[{"id":1}],"links":{"next":"?user_id=2&external_reference_id=tempo%3Av1%3Apart&cursor=second"}}`)
		} else {
			fmt.Fprint(w, `{"time_entries":[{"id":2}],"links":{"next":null}}`)
		}
	})
	rows, err := c.List(context.Background(), "/time_entries", url.Values{"user_id": {"2"}, "external_reference_id": {"tempo:v1:part"}})
	if err != nil || calls != 2 || len(rows) != 2 {
		t.Fatalf("incomplete pagination: rows=%v calls=%d err=%v", rows, calls, err)
	}
}
