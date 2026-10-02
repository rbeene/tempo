//go:build linux

package worker

import (
	"golang.org/x/sys/unix"
	"os"
)

func publishExclusive(root *os.Root, from, to string) error {
	d, e := root.Open(".")
	if e != nil {
		return e
	}
	defer d.Close()
	return unix.Renameat2(int(d.Fd()), from, int(d.Fd()), to, unix.RENAME_NOREPLACE)
}
