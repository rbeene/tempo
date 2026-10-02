//go:build !darwin && !linux

package activity

import (
	"errors"
	"os"
)

func privateInfo(os.FileInfo, bool) bool { return false }
func openNoFollow(*os.Root, string, int, os.FileMode) (*os.File, error) {
	return nil, errors.New("unsupported private store")
}
func tryLockFile(*os.File) (bool, error) { return false, errors.New("unsupported lock") }
func unlockFile(*os.File)                {}
