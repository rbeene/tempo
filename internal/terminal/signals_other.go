//go:build !darwin && !linux

package terminal

import "os"

func terminalSignals() (<-chan os.Signal, func()) { return nil, func() {} }
