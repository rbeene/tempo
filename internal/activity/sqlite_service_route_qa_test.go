package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSQLiteServiceConstructionAndUnportedRoutesNeverCreateLegacyAuthority(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fresh", "activity-state.json")
	s := NewSQLite(Options{Path: path})
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("construction touched absent directory: %v", err)
	}
	for _, create := range []bool{false, true} {
		owner, exists, err := s.store.acquire(context.Background(), create)
		var domain *Error
		if owner != nil || exists || !errors.As(err, &domain) || domain.Code != "unsupported_contract" {
			t.Fatalf("unported acquire(%v): owner=%v exists=%v error=%v", create, owner != nil, exists, err)
		}
	}
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unported operation created authority directory: %v", err)
	}
}

func TestSQLiteServiceUnportedReadAndUpdatePreserveExistingLegacyBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "activity-state.json")
	before := []byte("legacy bytes must not be parsed or replaced\n")
	if err := os.WriteFile(path, before, 0600); err != nil {
		t.Fatal(err)
	}
	s := NewSQLite(Options{Path: path})
	_, _, readErr := s.store.read(context.Background())
	callback := false
	writeErr := s.store.update(context.Background(), func(*state) (bool, error) { callback = true; return true, nil })
	for _, err := range []error{readErr, writeErr} {
		var domain *Error
		if !errors.As(err, &domain) || domain.Code != "unsupported_contract" {
			t.Fatalf("legacy route error: %v", err)
		}
	}
	if callback {
		t.Fatal("unported legacy mutation invoked callback")
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("legacy authority changed")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatal("unported operation created sidecars")
	}
}
