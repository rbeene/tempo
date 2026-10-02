// Package auth keeps credentials in a native credential store and only account
// selection in the filesystem. Constructing a store never accesses credentials.
package auth

import (
	"errors"
	"io"
	"strings"
	"unicode"
)

const maxTokenBytes = 16 * 1024

var ErrNotFound = errors.New("no saved token; log in or set HARVEST_TOKEN")
var errInvalidToken = errors.New("token must be a nonempty single value of at most 16 KiB")

type Store interface {
	Get() (string, error)
	Set(string) error
	Delete() error
}

// ReadToken bounds the total input before trimming surrounding whitespace.
func ReadToken(r io.Reader) (string, error) {
	if r == nil {
		return "", errInvalidToken
	}
	b, err := io.ReadAll(io.LimitReader(r, maxTokenBytes+1))
	if err != nil {
		return "", errors.New("cannot read token input")
	}
	if len(b) > maxTokenBytes {
		return "", errInvalidToken
	}
	return validateToken(string(b))
}

func validateToken(v string) (string, error) {
	if len(v) > maxTokenBytes {
		return "", errInvalidToken
	}
	v = strings.TrimSpace(v)
	if v == "" || strings.ContainsFunc(v, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) || r == unicode.ReplacementChar }) {
		return "", errInvalidToken
	}
	return v, nil
}

func ResolveToken(store Store, getenv func(string) string) (token, source string, err error) {
	if getenv != nil {
		if v := getenv("HARVEST_TOKEN"); v != "" {
			token, err = validateToken(v)
			if err != nil {
				return "", "", err
			}
			return token, "environment", nil
		}
	}
	if store == nil {
		return "", "", ErrNotFound
	}
	v, err := store.Get()
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", "", ErrNotFound
		}
		return "", "", errors.New("cannot read credential store; set HARVEST_TOKEN as an alternative")
	}
	token, err = validateToken(v)
	if err != nil {
		return "", "", errors.New("saved token is invalid; log in again")
	}
	return token, "keychain", nil
}
