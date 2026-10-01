// Package harvest provides the bounded, redacted Harvest API v2 transport.
package harvest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	maxBodyBytes  = 8 << 20
	maxPages      = 100
	maxAttempts   = 3
	maxRetryDelay = 5 * time.Second
)

// Object retains exact JSON numbers, including identifiers larger than 2^53.
type Object = map[string]any

// Provider is the API surface used by the CLI. List never returns partial results.
type Provider interface {
	Accounts(context.Context) ([]Object, error)
	Get(context.Context, string) (Object, error)
	List(context.Context, string, url.Values) ([]Object, error)
	Create(context.Context, string, Object) (Object, error)
	Update(context.Context, string, Object) (Object, error)
	Delete(context.Context, string) error
}

// Error is safe to display: it never contains upstream bodies, URLs, or errors.
// Uncertain means a write may have succeeded; inspect state before manual retry.
type Error struct {
	Code      string
	Message   string
	Status    int
	Retryable bool
	Uncertain bool
}

func (e *Error) Error() string { return e.Message }

// Client is immutable after construction and supports concurrent requests.
type Client struct {
	token, account string
	api, id        *url.URL
	http           *http.Client
	invalid        bool
}

var _ Provider = (*Client)(nil)

// New uses fixed production HTTPS origins. It does not read configuration.
func New(token, account string) *Client {
	return NewWithHTTP(token, account, "https://api.harvestapp.com/v2", "https://id.getharvest.com/api/v2", nil)
}

// NewWithHTTP supplies explicit bases and an HTTP client for isolated tests.
// Bases retain their path prefix (normally /v2 and /api/v2). Invalid bases fail
// before any request. The supplied client is copied and redirects are disabled.
func NewWithHTTP(token, account, apiBase, idBase string, httpClient *http.Client) *Client {
	c := &Client{token: token, account: account}
	c.api = parseBase(apiBase)
	c.id = parseBase(idBase)
	c.invalid = c.api == nil || c.id == nil
	h := http.Client{Timeout: 30 * time.Second}
	if httpClient != nil {
		h = *httpClient
		if h.Timeout <= 0 || h.Timeout > 30*time.Second {
			h.Timeout = 30 * time.Second
		}
	}
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.http = &h
	return c
}
func parseBase(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || strings.Contains(u.Path, "\\") {
		return nil
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	if u.Path != "" && path.Clean(u.Path) != u.Path {
		return nil
	}
	return u
}
func validation() *Error { return &Error{Code: "validation", Message: "Invalid API request."} }
func responseError() *Error {
	return &Error{Code: "response", Message: "Harvest returned an invalid or incomplete response."}
}
func networkError() *Error {
	return &Error{Code: "network", Message: "Harvest request failed or was canceled.", Retryable: true}
}
func uncertain(status int) *Error {
	return &Error{Code: "uncertain_write", Message: "The write outcome is uncertain. Inspect the record before retrying manually.", Status: status, Uncertain: true}
}
func (c *Client) endpoint(base *url.URL, p string, q url.Values) (*url.URL, error) {
	if c.invalid || base == nil || !strings.HasPrefix(p, "/") || strings.HasPrefix(p, "//") || strings.ContainsAny(p, "?#%\\") || path.Clean(p) != p {
		return nil, validation()
	}
	u := *base
	u.Path += p
	u.RawQuery = q.Encode()
	return &u, nil
}

// Accounts lists only Harvest accounts accessible to the token, without an account header.
func (c *Client) Accounts(ctx context.Context) ([]Object, error) {
	u, err := c.endpoint(c.id, "/accounts", nil)
	if err != nil {
		return nil, err
	}
	obj, err := c.request(ctx, http.MethodGet, u, nil, false)
	if err != nil {
		return nil, err
	}
	rows, err := objects(obj, "accounts")
	if err != nil {
		return nil, err
	}
	out := make([]Object, 0, len(rows))
	for _, row := range rows {
		if row["product"] == "harvest" {
			out = append(out, row)
		}
	}
	return out, nil
}

// Get retrieves one API object.
func (c *Client) Get(ctx context.Context, p string) (Object, error) {
	return c.call(ctx, http.MethodGet, p, nil)
}

// Create sends exactly one POST attempt.
func (c *Client) Create(ctx context.Context, p string, body Object) (Object, error) {
	return c.call(ctx, http.MethodPost, p, body)
}

// Update sends exactly one PATCH attempt, including timer restart and stop operations.
func (c *Client) Update(ctx context.Context, p string, body Object) (Object, error) {
	return c.call(ctx, http.MethodPatch, p, body)
}

// Delete sends exactly one DELETE attempt and accepts an empty successful response.
func (c *Client) Delete(ctx context.Context, p string) error {
	_, err := c.call(ctx, http.MethodDelete, p, nil)
	return err
}
func (c *Client) call(ctx context.Context, method, p string, body Object) (Object, error) {
	u, err := c.endpoint(c.api, p, nil)
	if err != nil {
		return nil, err
	}
	return c.request(ctx, method, u, body, true)
}

// List follows server-provided links.next, restricted to the initial origin and
// exact escaped path and initial non-pagination query values. Cursor URLs are
// never synthesized from next_page values.
func (c *Client) List(ctx context.Context, p string, q url.Values) ([]Object, error) {
	key := path.Base(p)
	switch key {
	case "project_assignments", "task_assignments", "time_entries", "projects", "tasks", "clients":
	default:
		return nil, validation()
	}
	initial, err := c.endpoint(c.api, p, q)
	if err != nil {
		return nil, err
	}
	initialScope, err := queryScope(initial.RawQuery)
	if err != nil {
		return nil, validation()
	}
	u := initial
	seen := map[string]bool{}
	out := make([]Object, 0)
	for page := 0; page < maxPages; page++ {
		canonical := *u
		query, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			return nil, responseError()
		}
		canonical.RawQuery = query.Encode()
		if seen[canonical.String()] {
			return nil, responseError()
		}
		seen[canonical.String()] = true
		obj, err := c.request(ctx, http.MethodGet, u, nil, true)
		if err != nil {
			return nil, err
		}
		rows, err := objects(obj, key)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		rawLinks, exists := obj["links"]
		if !exists {
			return nil, responseError()
		}
		links, ok := rawLinks.(map[string]any)
		if !ok {
			return nil, responseError()
		}
		rawNext, exists := links["next"]
		if !exists {
			return nil, responseError()
		}
		if rawNext == nil {
			return out, nil
		}
		next, ok := rawNext.(string)
		if !ok || next == "" {
			return nil, responseError()
		}
		ref, err := url.Parse(next)
		if err != nil || ref.User != nil || ref.Fragment != "" || strings.Contains(next, "#") {
			return nil, responseError()
		}
		candidate := u.ResolveReference(ref)
		if candidate.Scheme != initial.Scheme || candidate.Host != initial.Host || candidate.User != nil || candidate.EscapedPath() != initial.EscapedPath() || candidate.Path != initial.Path || candidate.Opaque != "" {
			return nil, responseError()
		}
		candidateScope, err := queryScope(candidate.RawQuery)
		if err != nil || candidateScope != initialScope {
			return nil, responseError()
		}
		u = candidate
	}
	return nil, responseError()
}

// queryScope retains every filter value, including repeated values and their
// order. Only Harvest pagination controls may change between pages.
func queryScope(raw string) (string, error) {
	query, err := url.ParseQuery(raw)
	if err != nil {
		return "", err
	}
	for _, control := range []string{"page", "cursor", "per_page", "ref"} {
		query.Del(control)
	}
	return query.Encode(), nil
}

func objects(obj Object, key string) ([]Object, error) {
	raw, ok := obj[key].([]any)
	if !ok {
		return nil, responseError()
	}
	out := make([]Object, 0, len(raw))
	for _, item := range raw {
		row, ok := item.(map[string]any)
		if !ok || row == nil {
			return nil, responseError()
		}
		out = append(out, row)
	}
	return out, nil
}

func (c *Client) request(ctx context.Context, method string, u *url.URL, body Object, account bool) (Object, error) {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return nil, validation()
		}
	}
	read := method == http.MethodGet
	attempts := 1
	if read {
		attempts = maxAttempts
	}
	for attempt := 0; attempt < attempts; attempt++ {
		if ctx.Err() != nil {
			return nil, networkError()
		}
		req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(data))
		if err != nil {
			return nil, validation()
		}
		// Do not supply GetBody: even the standard transport must not replay writes.
		if !read {
			req.GetBody = nil
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("User-Agent", "Tempo (https://github.com/rbeene/tempo)")
		req.Header.Set("Accept", "application/json")
		if account {
			req.Header.Set("Harvest-Account-Id", c.account)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			if !read {
				return nil, uncertain(0)
			}
			if attempt+1 < attempts && ctx.Err() == nil {
				if !wait(ctx, retryDelay("", attempt)) {
					return nil, networkError()
				}
				continue
			}
			return nil, networkError()
		}
		status := resp.StatusCode
		if status < 200 || status >= 300 {
			resp.Body.Close()
			if !read && status >= 500 {
				return nil, uncertain(status)
			}
			apiErr := statusError(status)
			if read && apiErr.Retryable && attempt+1 < attempts {
				if !wait(ctx, retryDelay(resp.Header.Get("Retry-After"), attempt)) {
					return nil, networkError()
				}
				continue
			}
			return nil, apiErr
		}
		payload, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		resp.Body.Close()
		if err != nil || len(payload) > maxBodyBytes {
			if !read {
				return nil, uncertain(status)
			}
			return nil, responseError()
		}
		if method == http.MethodDelete && len(bytes.TrimSpace(payload)) == 0 {
			return Object{}, nil
		}
		dec := json.NewDecoder(bytes.NewReader(payload))
		dec.UseNumber()
		var obj Object
		err = dec.Decode(&obj)
		var trailing any
		if err != nil || obj == nil || dec.Decode(&trailing) != io.EOF {
			if !read {
				return nil, uncertain(status)
			}
			return nil, responseError()
		}
		return obj, nil
	}
	return nil, networkError()
}
func statusError(status int) *Error {
	e := &Error{Code: "api", Message: "Harvest rejected the request.", Status: status}
	switch status {
	case 401:
		e.Code = "auth"
		e.Message = "Harvest authentication failed."
	case 403:
		e.Code = "forbidden"
		e.Message = "Harvest access is forbidden."
	case 404:
		e.Code = "not_found"
		e.Message = "Harvest record was not found."
	case 400, 422:
		e.Code = "validation"
		e.Message = "Harvest rejected the request values."
	case 429:
		e.Code = "rate_limit"
		e.Message = "Harvest rate limit reached."
		e.Retryable = true
	default:
		e.Retryable = status >= 500
	}
	return e
}
func retryDelay(value string, attempt int) time.Duration {
	delay := time.Duration(1<<attempt) * 100 * time.Millisecond
	if value != "" {
		if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
			if seconds >= int64(maxRetryDelay/time.Second) {
				return maxRetryDelay
			}
			return time.Duration(seconds) * time.Second
		}
		if date, err := http.ParseTime(value); err == nil {
			delay = time.Until(date)
		}
	}
	if delay < 0 {
		return 0
	}
	if delay > maxRetryDelay {
		return maxRetryDelay
	}
	return delay
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
