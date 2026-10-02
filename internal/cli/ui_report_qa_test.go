package cli

import (
	"bytes"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"strings"
	"testing"
)

func qaUIReportContains(t *testing.T, b *bytes.Buffer, needles ...string) {
	t.Helper()
	s := strings.ToLower(b.String())
	if s == "" {
		t.Fatal("post-restoration report missing")
	}
	for _, n := range needles {
		if !strings.Contains(s, strings.ToLower(n)) {
			t.Errorf("report hides %q: %s", n, s)
		}
	}
	for _, n := range []string{"RAW-REPORT-SECRET", "\x1b", "RAW-REQUEST-SECRET", "RAW-OP-SECRET"} {
		if strings.Contains(b.String(), n) {
			t.Errorf("unsafe diagnostic %q", b.String())
		}
	}
}
func TestQAUIAuthReportsExactSafeSharedEffects(t *testing.T) {
	for _, mode := range []string{"success", "definitive-failure"} {
		t.Run(mode, func(t *testing.T) {
			var b bytes.Buffer
			result := auth.Result{AccountID: "11", Authenticated: true, Source: "keychain", Effects: auth.Effects{Credential: "applied", Config: "saved"}}
			var e error
			if mode == "definitive-failure" {
				result = auth.Result{}
				e = &auth.Error{Code: "config", Message: "RAW-REPORT-SECRET\x1b]52;payload\x07", Effects: auth.Effects{Credential: "unchanged", Config: "restored"}}
			}
			uiAuthReport(&b, "login", result, e)
			if mode == "success" {
				qaUIReportContains(t, &b, "login", "11", "applied", "saved")
			} else {
				qaUIReportContains(t, &b, "login", "config", "unchanged", "restored")
			}
		})
	}
}
func TestQAUIRetainedReportsSafeAuthEffectsAndCanonicalLocalRequest(t *testing.T) {
	for _, mode := range []string{"auth", "links", "non-v4", "invalid-request"} {
		t.Run(mode, func(t *testing.T) {
			var b bytes.Buffer
			if mode == "auth" {
				uiRetainedReport(&b, "auth", "", &auth.Error{Code: "credential_write_unknown", Uncertain: true, Message: "RAW-REPORT-SECRET", Effects: auth.Effects{Credential: "unknown", Config: "saved"}})
				qaUIReportContains(t, &b, "credential_write_unknown", "unknown", "saved")
				if strings.Contains(strings.ToLower(b.String()), "replay") {
					t.Error("auth diagnostic invented ledger replay")
				}
			} else {
				id := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
				if mode == "non-v4" {
					id = "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa"
				}
				if mode == "invalid-request" {
					id = "RAW-REQUEST-SECRET\x1b[31m"
				}
				uiRetainedReport(&b, "links", id, &activity.Error{Code: "local_write_unknown", Uncertain: true, Message: "RAW-REPORT-SECRET", Details: map[string]any{"request_id": id, "raw": "RAW-REPORT-SECRET"}})
				qaUIReportContains(t, &b, "local_write_unknown")
				if mode == "links" || mode == "non-v4" {
					qaUIReportContains(t, &b, id)
				}
			}
		})
	}
}
func TestQAUIRestorationReportFixedSafeNotice(t *testing.T) {
	var b bytes.Buffer
	uiRestorationReport(&b)
	qaUIReportContains(t, &b, "terminal", "restoration", "failed")
	if strings.Count(b.String(), "\n") != 1 {
		t.Errorf("restoration notice should be one bounded line: %q", b.String())
	}
}

func TestQAUIResultErrorRetainsSupportedNonV4RequestIdentity(t *testing.T) {
	id := "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa"
	mapped := uiResultError(&activity.Error{Code: "local_write_unknown", Uncertain: true, Message: "RAW-REPORT-SECRET", Details: map[string]any{"request_id": id}})
	if !strings.Contains(mapped.Error(), id) || strings.Contains(mapped.Error(), "RAW-REPORT-SECRET") {
		t.Errorf("supported shared request ID omitted from safe human diagnostic: %v", mapped)
	}
}

func TestQAUIResultErrorAuthUnknownProjectsSafeMessageAndExactEffects(t *testing.T) {
	original := &auth.Error{Code: "credential_write_unknown", Uncertain: true, Message: "RAW-REPORT-SECRET\x1b]52;payload\x07", Effects: auth.Effects{Credential: "unknown", Config: "saved"}}
	mapped := uiResultError(original)
	projected, ok := mapped.(*auth.Error)
	if !ok || projected.Code != original.Code || projected.Uncertain != original.Uncertain || projected.Retryable != original.Retryable || projected.Effects != original.Effects {
		t.Fatalf("typed auth classification/effects changed %#v", mapped)
	}
	if strings.Contains(projected.Message, "RAW-REPORT-SECRET") || strings.Contains(projected.Message, "\x1b") || !strings.Contains(strings.ToLower(projected.Message), "inspect") {
		t.Errorf("unsafe auth primary projection: %q", projected.Message)
	}
	if original.Message != "RAW-REPORT-SECRET\x1b]52;payload\x07" {
		t.Error("CLI projection mutated original retained outcome")
	}
}
