//go:build !darwin && !linux

package terminal

import (
	"errors"
	"os"
)

func duplicateTerminal(*os.File) (*os.File, func(), error) {
	return nil, nil, errors.New("interactive terminal unavailable")
}
