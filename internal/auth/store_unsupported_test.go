//go:build !darwin || !cgo

package auth

import (
	"strings"
	"testing"
)

func TestUnsupportedStoreExplainsEnvironmentAlternative(t *testing.T) {
	store := NewStore()
	_, getErr := store.Get()
	for _, err := range []error{getErr, store.Set("test-value"), store.Delete()} {
		if err == nil || !strings.Contains(err.Error(), "HARVEST_TOKEN") {
			t.Fatal("unsupported store must explain alternative")
		}
	}
	token, _, err := ResolveToken(store, func(string) string { return "test-environment-token" })
	if err != nil || token != "test-environment-token" {
		t.Fatal("environment alternative failed", err)
	}
}
