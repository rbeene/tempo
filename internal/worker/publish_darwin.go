//go:build darwin

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
	return unix.RenameatxNp(int(d.Fd()), from, int(d.Fd()), to, unix.RENAME_EXCL)
}
