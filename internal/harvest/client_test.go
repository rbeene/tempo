package harvest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func testClient(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return NewWithHTTP("secret-token", "42", s.URL+"/v2", s.URL+"/api/v2", s.Client())
}
func errorCode(t *testing.T, err error, code string) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("error=%v, want %s", err, code)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatal("secret leaked")
	}
	return e
}
func TestCursorAndNumbers(t *testing.T) {
	calls := 0
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer secret-token" || r.Header.Get("Harvest-Account-Id") != "42" || r.Header.Get("User-Agent") == "" {
			t.Error("headers")
		}
		if calls == 1 {
			if r.URL.Query().Get("user_id") != "7" {
				t.Error("query lost")
			}
			fmt.Fprint(w, `{"time_entries":[{"id":9007199254740993}],"next_page":999,"links":{"next":"?user_id=7&cursor=abc"}}`)
		} else {
			if r.URL.Query().Get("cursor") != "abc" || r.URL.Query().Get("page") != "" {
				t.Error("cursor not followed")
			}
			fmt.Fprint(w, `{"time_entries":[{"id":2}],"links":{"next":null}}`)
		}
	})
	rows, err := c.List(context.Background(), "/time_entries", url.Values{"user_id": {"7"}})
	if err != nil || len(rows) != 2 || rows[0]["id"] != json.Number("9007199254740993") || calls != 2 {
		t.Fatalf("rows=%v err=%v calls=%d", rows, err, calls)
	}
}
func TestUnsafePaginationNoPartialResults(t *testing.T) {
	for _, next := range []string{"https://evil.example/v2/time_entries", "/v2/projects", "//evil.example/v2/time_entries", "/v2/time_entries#fragment", "/v2/%74ime_entries", "?cursor=x"} {
		t.Run(next, func(t *testing.T) {
			calls := 0
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				fmt.Fprintf(w, `{"time_entries":[{"id":1}],"links":{"next":%q}}`, next)
			})
			rows, err := c.List(context.Background(), "/time_entries", nil)
			errorCode(t, err, "response")
			if rows != nil || calls > 2 {
				t.Fatalf("partial results or unsafe requests: %v %d", rows, calls)
			}
		})
	}
}
func TestAccountsFilter(t *testing.T) {
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/accounts" || r.Header.Get("Harvest-Account-Id") != "" {
			t.Error("accounts request")
		}
		fmt.Fprint(w, `{"accounts":[{"id":1,"product":"harvest"},{"id":2,"product":"forecast"}]}`)
	})
	rows, err := c.Accounts(context.Background())
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v %v", rows, err)
	}
}
func TestReadRetriesWriteNeverReplays(t *testing.T) {
	for _, method := range []string{"GET", "POST", "PATCH", "DELETE"} {
		t.Run(method, func(t *testing.T) {
			calls := 0
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(503)
				fmt.Fprint(w, "secret-token")
			})
			var err error
			switch method {
			case "GET":
				_, err = c.Get(context.Background(), "/users/me")
			case "POST":
				_, err = c.Create(context.Background(), "/time_entries", Object{})
			case "PATCH":
				_, err = c.Update(context.Background(), "/time_entries/1", Object{})
			case "DELETE":
				err = c.Delete(context.Background(), "/time_entries/1")
			}
			if method == "GET" {
				errorCode(t, err, "api")
				if calls != 3 {
					t.Fatalf("calls=%d", calls)
				}
			} else {
				e := errorCode(t, err, "uncertain_write")
				if calls != 1 || !e.Uncertain || e.Retryable {
					t.Fatalf("calls=%d err=%+v", calls, e)
				}
			}
		})
	}
}
func TestErrorsAndRedirectSuppressed(t *testing.T) {
	for status, code := range map[int]string{401: "auth", 403: "forbidden", 404: "not_found", 422: "validation", 429: "rate_limit", 302: "api"} {
		t.Run(code, func(t *testing.T) {
			calls := 0
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Location", "/secret-token")
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(status)
				fmt.Fprint(w, `{"error":"secret-token"}`)
			})
			_, err := c.Get(context.Background(), "/users/me")
			errorCode(t, err, code)
			if status == 302 && calls != 1 {
				t.Fatal("redirect followed")
			}
		})
	}
}
func TestCanceledRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
		cancel()
	})
	start := time.Now()
	_, err := c.Get(ctx, "/users/me")
	errorCode(t, err, "network")
	if time.Since(start) > time.Second {
		t.Fatal("cancellation slow")
	}
}
func TestMalformedWritesUncertain(t *testing.T) {
	for _, body := range []string{"secret-token", "null", "{} {}"} {
		t.Run(body, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
			_, err := c.Create(context.Background(), "/time_entries", Object{})
			errorCode(t, err, "uncertain_write")
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestTransportRedactedAndWriteUncertain(t *testing.T) {
	calls := 0
	c := NewWithHTTP("secret-token", "42", "https://example.test/v2", "https://example.test/api/v2", &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) { calls++; return nil, errors.New("secret-token") })})
	_, err := c.Create(context.Background(), "/time_entries", Object{})
	errorCode(t, err, "uncertain_write")
	if calls != 1 {
		t.Fatal("write replayed")
	}
}
func TestBodyAndPageBounds(t *testing.T) {
	t.Run("body", func(t *testing.T) {
		c := testClient(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, strings.Repeat(" ", maxBodyBytes+1)) })
		_, err := c.Get(context.Background(), "/users/me")
		errorCode(t, err, "response")
	})
	t.Run("pages", func(t *testing.T) {
		n := 0
		c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
			n++
			fmt.Fprintf(w, `{"tasks":[],"links":{"next":"?cursor=%d"}}`, n)
		})
		rows, err := c.List(context.Background(), "/tasks", nil)
		errorCode(t, err, "response")
		if rows != nil || n != maxPages {
			t.Fatalf("rows=%v requests=%d", rows, n)
		}
	})
}
func TestInvalidBasesAndPathsNoRequests(t *testing.T) {
	for _, base := range []string{"https://user:secret@example.test/v2", "ftp://example.test/v2", "https://example.test/v2?secret=x", "https://example.test/v2#secret"} {
		c := NewWithHTTP("x", "42", base, base, nil)
		_, err := c.Get(context.Background(), "/users/me")
		errorCode(t, err, "validation")
	}
	c := New("x", "42")
	for _, path := range []string{"https://evil.example/a", "//evil.example", "/../users/me", "/users/me?secret=x", "/users/%6de"} {
		_, err := c.Get(context.Background(), path)
		errorCode(t, err, "validation")
	}
}

func TestSuccessfulWrites(t *testing.T) {
	for _, method := range []string{"POST", "PATCH", "DELETE"} {
		t.Run(method, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != method {
					t.Errorf("method=%s", r.Method)
				}
				if method == "DELETE" {
					w.WriteHeader(204)
					return
				}
				var body Object
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["notes"] != "work" || r.Header.Get("Content-Type") != "application/json" {
					t.Error("incorrect JSON body")
				}
				if method == "POST" {
					w.WriteHeader(http.StatusCreated)
				}
				fmt.Fprint(w, `{"id":9007199254740993}`)
			})
			var obj Object
			var err error
			switch method {
			case "POST":
				obj, err = c.Create(context.Background(), "/time_entries", Object{"notes": "work"})
			case "PATCH":
				obj, err = c.Update(context.Background(), "/time_entries/1", Object{"notes": "work"})
			case "DELETE":
				err = c.Delete(context.Background(), "/time_entries/1")
			}
			if err != nil {
				t.Fatal(err)
			}
			if method != "DELETE" && obj["id"] != json.Number("9007199254740993") {
				t.Fatal(obj)
			}
		})
	}
}

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, errors.New("secret-token") }
func (failingBody) Close() error             { return nil }
func TestUnreadableWriteUncertain(t *testing.T) {
	calls := 0
	c := NewWithHTTP("secret-token", "42", "https://example.test/v2", "https://example.test/api/v2", &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: failingBody{}, Request: r}, nil
	})})
	_, err := c.Update(context.Background(), "/time_entries/1", Object{})
	errorCode(t, err, "uncertain_write")
	if calls != 1 {
		t.Fatal("write replayed")
	}
}
func TestMalformedListNoPartialSuccess(t *testing.T) {
	for _, body := range []string{`{"tasks":null,"links":{"next":null}}`, `{"tasks":[null],"links":{"next":null}}`, `{"tasks":[],"next_page":2}`, `{"tasks":[]}`, `{"tasks":[],"links":{}}`, `{"tasks":[],"links":{"next":3}}`} {
		t.Run(body, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) })
			rows, err := c.List(context.Background(), "/tasks", nil)
			errorCode(t, err, "response")
			if rows != nil {
				t.Fatal("partial success")
			}
		})
	}
}
func TestRetryAfterBounds(t *testing.T) {
	for _, value := range []string{"999999999", "99999999999999999999999", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)} {
		if d := retryDelay(value, 0); d < 0 || d > maxRetryDelay {
			t.Fatalf("unbounded delay %v", d)
		}
	}
	if retryDelay("0", 0) != 0 {
		t.Fatal("zero Retry-After ignored")
	}
	if d := retryDelay(time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat), 0); d != 0 {
		t.Fatal("past date must not wait")
	}
}
func TestCanceledBeforeWriteNotUncertain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := New("secret-token", "42")
	_, err := c.Create(ctx, "/time_entries", Object{})
	e := errorCode(t, err, "network")
	if e.Uncertain {
		t.Fatal("unsent write is not uncertain")
	}
}

func TestPaginationPreservesQueryScope(t *testing.T) {
	initial := url.Values{"user_id": {"7"}, "from": {"2026-10-01"}, "to": {"2026-10-02"}, "is_running": {"true"}}
	for _, scenario := range []struct {
		name  string
		alter func(url.Values)
	}{
		{"dropped user", func(q url.Values) { q.Del("user_id") }},
		{"changed user", func(q url.Values) { q.Set("user_id", "8") }},
		{"duplicated user", func(q url.Values) { q.Add("user_id", "7") }},
		{"dropped from", func(q url.Values) { q.Del("from") }},
		{"changed to", func(q url.Values) { q.Set("to", "2026-10-01") }},
		{"changed running", func(q url.Values) { q.Set("is_running", "false") }},
		{"added filter", func(q url.Values) { q.Set("project_id", "99") }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			calls, writes := 0, 0
			next, _ := url.ParseQuery(initial.Encode())
			next.Set("cursor", "abc")
			scenario.alter(next)
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodGet {
					writes++
				}
				fmt.Fprintf(w, `{"time_entries":[{"id":1}],"links":{"next":%q}}`, "?"+next.Encode())
			})
			rows, err := c.List(context.Background(), "/time_entries", initial)
			errorCode(t, err, "response")
			if rows != nil || calls != 1 || writes != 0 {
				t.Fatalf("rows=%v calls=%d writes=%d", rows, calls, writes)
			}
		})
	}
}

func TestPaginationAllowsControlsAndPreservesDuplicateValues(t *testing.T) {
	calls := 0
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			fmt.Fprint(w, `{"time_entries":[],"links":{"next":"?user_id=7&user_id=8&cursor=abc&per_page=100&ref=next_cursor"}}`)
		} else {
			if r.URL.Query().Get("ref") != "next_cursor" {
				t.Fatal("continuation controls lost")
			}
			fmt.Fprint(w, `{"time_entries":[],"links":{"next":null}}`)
		}
	})
	_, err := c.List(context.Background(), "/time_entries", url.Values{"user_id": {"7", "8"}, "page": {"1"}})
	if err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
