//go:build !darwin && !linux

package hookstate

import "os"

func privateInfo(os.FileInfo, bool) bool { return false }
func openNoFollow(*os.Root, string, int, os.FileMode) (*os.File, error) {
	return nil, problem("unsupported_contract")
}
func tryLockFile(*os.File) (bool, error) { return false, problem("unsupported_contract") }
func unlockFile(*os.File)                {}

func repositoryIdentity(fi os.FileInfo) string { return "" }
