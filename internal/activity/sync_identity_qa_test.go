package activity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
)

type qaSyncIdentityProvider struct {
	*qaSyncProvider
	user      string
	afterRead func()
}

func (p *qaSyncIdentityProvider) Get(ctx context.Context, path string) (harvest.Object, error) {
	if path != "/users/me" {
		return p.qaSyncProvider.Get(ctx, path)
	}
	p.calls = append(p.calls, path)
	if p.afterRead != nil {
		p.afterRead()
	}
	return harvest.Object{"id": p.user, "is_active": true, "timezone": "UTC", "private": "SYNC-IDENTITY-SECRET-CANARY"}, nil
}
func qaSyncIdentityDeps(t *testing.T, p harvest.Provider) SyncDependencies {
	return SyncDependencies{NewProvider: func(ctx context.Context, account string) (harvest.Provider, error) {
		if account != "1" {
			t.Errorf("identity discovered wrong account %q", account)
		}
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 2*time.Minute {
			t.Error("identity remote read not bounded")
		}
		return p, nil
	}}
}
func TestQASyncIdentityReadOnlyVerifiedScope(t *testing.T) {
	s, path := qaLinkService(t)
	p := &qaSyncIdentityProvider{qaSyncProvider: qaNewSyncProvider(t), user: "2"}
	result, e := s.SyncIdentity(context.Background(), "1", qaSyncIdentityDeps(t, p))
	if e != nil || result != (SyncAccountIdentity{AccountID: "1", UserID: "2"}) {
		t.Fatalf("verified scope lost: %+v %v", result, e)
	}
	if !reflect.DeepEqual(p.calls, []string{"accounts", "/users/me"}) {
		t.Errorf("identity bypassed exact Harvest/current-user proof: %v", p.calls)
	}
	b, e := json.Marshal(result)
	if e != nil || string(b) != `{"account_id":"1","user_id":"2"}` {
		t.Errorf("identity disclosed non-contract fields: %s %v", b, e)
	}
	if _, e := os.Stat(path); !errors.Is(e, os.ErrNotExist) {
		t.Error("read-only identity persisted local state")
	}
}
func TestQASyncIdentityRejectsInvalidAndCanceledBeforeDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name, account, code string
		canceled            bool
	}{
		{"empty", "", "input_required", false}, {"noncanonical", "01", "validation", false}, {"canceled", "1", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, path := qaLinkService(t)
			calls := 0
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("synthetic canceled identity")
			if tc.canceled {
				cancel(cause)
			}
			result, e := s.SyncIdentity(ctx, tc.account, SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) {
				calls++
				return nil, errors.New("untrusted provider detail")
			}})
			if tc.canceled {
				if e == nil {
					t.Error("canceled identity reported success")
				}
			} else {
				qaCode(t, e, tc.code)
			}
			if result != (SyncAccountIdentity{}) || calls != 0 {
				t.Errorf("invalid identity discovered or disclosed result: %+v calls%d", result, calls)
			}
			if _, e := os.Stat(path); !errors.Is(e, os.ErrNotExist) {
				t.Error("invalid identity persisted state")
			}
		})
	}
}
func TestQASyncConfigureExpectedUserRejectsEqualRevisionOtherUser(t *testing.T) {
	s, path, _ := qaSyncFixture(t, time.Minute)
	base := SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: qaSyncID(851), Confirmed: true}
	provider := &qaSyncIdentityProvider{qaSyncProvider: qaNewSyncProvider(t), user: "2"}
	if _, e := s.SyncConfigure(context.Background(), base, qaSyncIdentityDeps(t, provider)); e != nil {
		t.Fatal(e)
	}
	provider.user = "3"
	other := base
	other.UserID = "3"
	other.RequestID = qaSyncID(852)
	if _, e := s.SyncConfigure(context.Background(), other, qaSyncIdentityDeps(t, provider)); e != nil {
		t.Fatal(e)
	}
	status, e := s.SyncStatus(context.Background())
	if e != nil || len(status.Configurations) != 2 {
		t.Fatalf("invalid equal-revision setup: %+v %v", status, e)
	}
	for _, c := range status.Configurations {
		if c.Revision != "1" {
			t.Fatal("setup scopes do not have equal revision")
		}
	}
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	guarded := base
	guarded.IfRevision = "1"
	guarded.RequestID = qaSyncID(853)
	guarded.DurationPolicy = "nearest-hundredth-hour"
	_, e = s.SyncConfigure(context.Background(), guarded, qaSyncIdentityDeps(t, provider))
	qaCode(t, e, "identity_conflict")
	after, readErr := os.ReadFile(path)
	if readErr != nil || !bytes.Equal(before, after) {
		t.Error("different current user mutated equal-revision consent or receipt")
	}
	if len(provider.posts) != 0 {
		t.Error("identity guard dispatched remote write")
	}
}
func TestQASyncConfigureGuardValidationAndCancellationDoNotWrite(t *testing.T) {
	for _, mode := range []string{"invalid-user", "cancel-during-user"} {
		t.Run(mode, func(t *testing.T) {
			s, path, _ := qaSyncFixture(t, time.Minute)
			before, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			in := SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: qaSyncID(854), Confirmed: true}
			p := &qaSyncIdentityProvider{qaSyncProvider: qaNewSyncProvider(t), user: "2"}
			calls := 0
			if mode == "invalid-user" {
				in.UserID = "02"
			} else {
				p.afterRead = func() { cancel(errors.New("synthetic canceled user lookup")) }
			}
			deps := SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { calls++; return p, nil }}
			_, e = s.SyncConfigure(ctx, in, deps)
			if mode == "invalid-user" {
				qaCode(t, e, "validation")
				if calls != 0 {
					t.Error("malformed expected user constructed credential provider")
				}
			} else if e == nil {
				t.Error("canceled scope lookup committed configuration")
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil || !bytes.Equal(before, after) {
				t.Error("invalid/canceled guard changed state bytes")
			}
		})
	}
}
func TestQASyncConfigureHistoricalEmptyGuardFingerprintAndOfflineReplay(t *testing.T) {
	s, path, _ := qaSyncFixture(t, time.Minute)
	in := SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: qaSyncID(855), Confirmed: true}
	// This is the wire fingerprint input from before the optional UserID field.
	oldJSON := `{"Operation":"sync.configure","Input":{"AccountID":"1","Mode":"duration","DurationPolicy":"exact","Clock":"","IfRevision":"0","RequestID":"90000000-0000-4000-8000-000000000855","Confirmed":true}}`
	digest := sha256.Sum256([]byte(oldJSON))
	oldFingerprint := hex.EncodeToString(digest[:])
	result, e := s.SyncConfigure(context.Background(), in, qaSyncDeps(t, qaNewSyncProvider(t)))
	if e != nil {
		t.Fatal(e)
	}
	st, _, e := s.store.read(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if st.Requests[in.RequestID].Fingerprint != oldFingerprint {
		t.Fatalf("legacy configure receipt changed fingerprint: %s", st.Requests[in.RequestID].Fingerprint)
	}
	replay, e := qaLegacyNew(Options{Path: path}).SyncConfigure(context.Background(), in, qaSyncNoProvider(t))
	if e != nil || !reflect.DeepEqual(result, replay) {
		t.Fatalf("historical exact offline replay failed: %+v %v", replay, e)
	}
	guarded := in
	guarded.UserID = "2"
	_, e = qaLegacyNew(Options{Path: path}).SyncConfigure(context.Background(), guarded, qaSyncNoProvider(t))
	qaCode(t, e, "request_conflict")
}
func TestQASyncConfigureGuardedReceiptReplaysWithoutIdentityRediscovery(t *testing.T) {
	s, path, _ := qaSyncFixture(t, time.Minute)
	in := SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: qaSyncID(856), Confirmed: true}
	result, e := s.SyncConfigure(context.Background(), in, qaSyncDeps(t, qaNewSyncProvider(t)))
	if e != nil {
		t.Fatal(e)
	}
	replay, e := qaLegacyNew(Options{Path: path}).SyncConfigure(context.Background(), in, qaSyncNoProvider(t))
	if e != nil || !reflect.DeepEqual(result, replay) {
		t.Fatalf("exact guarded replay rediscovered scope: %+v %v", replay, e)
	}
	changed := in
	changed.UserID = "3"
	_, e = qaLegacyNew(Options{Path: path}).SyncConfigure(context.Background(), changed, qaSyncNoProvider(t))
	qaCode(t, e, "request_conflict")
	data, e := json.Marshal(in)
	if e != nil || !strings.HasSuffix(string(data), `,"Confirmed":true,"UserID":"2"}`) {
		t.Errorf("nonempty guard did not append to existing input identity: %s", data)
	}
}
