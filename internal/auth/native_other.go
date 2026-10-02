//go:build !darwin || !cgo

package auth

func NativeSupported() bool               { return false }
func noninteractiveStore() (Store, error) { return nil, issue("keychain", unchanged()) }
