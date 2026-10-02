//go:build !darwin || !cgo

package auth

import "errors"

type unsupportedStore struct{}

func NewStore() Store                         { return unsupportedStore{} }
func (unsupportedStore) Get() (string, error) { return "", unsupported() }
func (unsupportedStore) Set(string) error     { return unsupported() }
func (unsupportedStore) Delete() error        { return unsupported() }
func unsupported() error {
	return errors.New("native macOS Keychain requires macOS with cgo; set HARVEST_TOKEN instead")
}
