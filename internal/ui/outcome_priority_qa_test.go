package ui

import (
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"testing"
)

func TestQARetainedOutcomeAuthFirstAndDeterministicOtherFamilies(t *testing.T) {
	credential := &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "saved"}}
	links := &activity.Error{Code: "local_write_unknown", Uncertain: true, Details: map[string]any{"request_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}}
	capture := &activity.Error{Code: "local_write_unknown", Uncertain: true, Details: map[string]any{"request_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}}
	for _, tc := range []struct {
		name     string
		retained map[string]error
		want     error
	}{{"empty", nil, nil}, {"only-links", map[string]error{"links": links}, links}, {"auth-stronger", map[string]error{"links": links, "auth": credential, "activity": capture}, credential}, {"other-families-stable", map[string]error{"links": links, "activity": capture}, capture}} {
		t.Run(tc.name, func(t *testing.T) {
			for n := 0; n < 32; n++ {
				if got := primaryOutcome(tc.retained); got != tc.want {
					t.Fatalf("retained classification=%#v wantoriginal%#v", got, tc.want)
				}
			}
		})
	}
}
