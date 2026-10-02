//go:build !darwin && !linux

package privatefs

import (
	"errors"
	"os"
)

func PrivateInfo(os.FileInfo, bool) bool { return false }
func OpenNoFollow(*os.Root, string, int, os.FileMode) (*os.File, error) {
	return nil, errors.New("unsupported private store")
}
func TryLock(*os.File) (bool, error) { return false, errors.New("unsupported lock") }
func Unlock(*os.File)                {}

func directoryInfo(os.FileInfo, bool) bool { return false }

func SocketInfo(os.FileInfo) bool { return false }
