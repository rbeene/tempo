//go:build !darwin && !linux

package cli

import (
	"os"
	"time"
)

func deadlineActivityFile(file *os.File, deadline time.Time) (*os.File, func(), error) {
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if info.Mode().IsRegular() {
		return file, func() {}, nil
	}
	if err := file.SetReadDeadline(deadline); err != nil {
		return nil, nil, err
	}
	return file, func() { _ = file.SetReadDeadline(time.Time{}) }, nil
}
