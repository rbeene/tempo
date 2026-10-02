package activity

import (
	"github.com/rbeene/tempo/internal/privatefs"
	"os"
)

func privateInfo(f os.FileInfo, d bool) bool { return privatefs.PrivateInfo(f, d) }
func openNoFollow(r *os.Root, n string, f int, m os.FileMode) (*os.File, error) {
	return privatefs.OpenNoFollow(r, n, f, m)
}
func tryLockFile(f *os.File) (bool, error) { return privatefs.TryLock(f) }
func unlockFile(f *os.File)                { privatefs.Unlock(f) }
