//go:build darwin || linux

package terminal

import (
	"os"
	"os/signal"
	"syscall"
)

// Subscribe before raw mode and release only after restoration. SIGTSTP is
// deliberately consumed (even when its bounded channel is full): suspending a
// raw terminal while a shared operation runs would strand terminal ownership.
func terminalSignals() (<-chan os.Signal, func()) {
	resize := make(chan os.Signal, 1)
	suspend := make(chan os.Signal, 1)
	signal.Notify(suspend, syscall.SIGTSTP)
	signal.Notify(resize, syscall.SIGWINCH)
	return resize, func() { signal.Stop(resize); signal.Stop(suspend) }
}
