//go:build darwin || linux

package hookstate

import (
	"os"
	"syscall"
)

func openArtifact(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}
