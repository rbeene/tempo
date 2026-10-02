//go:build (!darwin && !linux) || (darwin && !cgo)

package activity

import "errors"

func platformSample() (string, uint64, uint64, error) {
	return "", 0, 0, errors.New("clock unsupported")
}
