package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/rbeene/tempo/internal/privatefs"
)

const maxControlBytes = 16 << 20
const maxReceipts = 4096

type heldFile struct {
	root *os.Root
	file *os.File
	dir  string
	info os.FileInfo
	name string
}

func (h *heldFile) close() { privatefs.Unlock(h.file); h.file.Close(); h.root.Close() }
func (h *heldFile) verify() error {
	current, e := os.Lstat(h.dir)
	if e != nil || !os.SameFile(current, h.info) {
		return issue("state_corrupt")
	}
	a, e := h.file.Stat()
	if e != nil {
		return issue("state_corrupt")
	}
	b, e := h.root.Lstat(h.name + ".lock")
	if e != nil || !privatefs.PrivateInfo(a, false) || !os.SameFile(a, b) {
		return issue("state_corrupt")
	}
	return nil
}
func acquire(ctx context.Context, path string) (*heldFile, error) {
	dir, name := filepath.Dir(path), filepath.Base(path)
	root, info, e := privatefs.OpenDirectory(dir, true)
	if e != nil {
		return nil, fileError(e)
	}
	f, e := privatefs.OpenNoFollow(root, name+".lock", os.O_RDWR|os.O_CREATE, 0600)
	if e != nil {
		root.Close()
		return nil, issue("state_corrupt")
	}
	h := &heldFile{root: root, file: f, dir: dir, info: info, name: name}
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	for {
		if bounded.Err() != nil {
			f.Close()
			root.Close()
			return nil, issue("state_busy")
		}
		ok, e := privatefs.TryLock(f)
		if e != nil {
			f.Close()
			root.Close()
			return nil, issue("state_corrupt")
		}
		if ok {
			break
		}
		select {
		case <-bounded.Done():
		case <-time.After(5 * time.Millisecond):
		}
	}
	if e = h.verify(); e != nil {
		h.close()
		return nil, e
	}
	return h, nil
}
func fileError(e error) error {
	if errors.Is(e, privatefs.ErrDurability) {
		return issue("local_write_unknown")
	}
	return issue("state_corrupt")
}
func readPrivate(root *os.Root, name string, limit int) ([]byte, error) {
	f, e := privatefs.OpenNoFollow(root, name, os.O_RDONLY, 0)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	fi, e := f.Stat()
	if e != nil || !privatefs.PrivateInfo(fi, false) || fi.Size() > int64(limit) {
		return nil, issue("state_corrupt")
	}
	b, e := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if e != nil || len(b) > limit {
		return nil, issue("state_corrupt")
	}
	after, e := root.Lstat(name)
	if e != nil || !os.SameFile(fi, after) {
		return nil, issue("state_corrupt")
	}
	return b, nil
}
func readPath(path string, limit int) ([]byte, error) {
	root, info, e := privatefs.OpenDirectory(filepath.Dir(path), false)
	if e != nil {
		return nil, e
	}
	defer root.Close()
	b, e := readPrivate(root, filepath.Base(path), limit)
	if e != nil {
		return nil, e
	}
	now, e := os.Lstat(filepath.Dir(path))
	if e != nil || !os.SameFile(info, now) {
		return nil, issue("state_corrupt")
	}
	return b, nil
}
func decode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		return issue("state_corrupt")
	}
	return nil
}
func atomicWrite(root *os.Root, name string, b []byte, verify func() error, fault func(string) error) error {
	return writeAtomic(root, name, b, verify, fault, false)
}
func writeAtomic(root *os.Root, name string, b []byte, verify func() error, fault func(string) error, exclusive bool) error {
	if e := verify(); e != nil {
		return e
	}
	var nonce [16]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return issue("state_corrupt")
	}
	tmp := ".tempo-" + hex.EncodeToString(nonce[:]) + ".tmp"
	f, e := privatefs.OpenNoFollow(root, tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return issue("state_corrupt")
	}
	defer root.Remove(tmp)
	if e = fault("before_write"); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = fault("file_sync")
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return issue("local_write_unknown")
	}
	if e = verify(); e != nil {
		return e
	}
	if e = fault("rename"); e != nil {
		return e
	}
	if exclusive {
		e = publishExclusive(root, tmp, name)
	} else {
		e = root.Rename(tmp, name)
	}
	if errors.Is(e, os.ErrExist) {
		return issue("revision_conflict")
	}
	if e != nil {
		return issue("local_write_unknown")
	}
	if e = fault("directory_sync"); e != nil {
		return e
	}
	d, e := root.Open(".")
	if e != nil {
		return issue("local_write_unknown")
	}
	defer d.Close()
	if d.Sync() != nil {
		return issue("local_write_unknown")
	}
	return nil
}
func syncExisting(h *heldFile) error {
	if e := h.verify(); e != nil {
		return e
	}
	f, e := privatefs.OpenNoFollow(h.root, h.name, os.O_RDONLY, 0)
	if e != nil {
		return issue("local_write_unknown")
	}
	e = f.Sync()
	f.Close()
	if e != nil {
		return issue("local_write_unknown")
	}
	d, e := h.root.Open(".")
	if e != nil {
		return issue("local_write_unknown")
	}
	e = d.Sync()
	d.Close()
	if e != nil {
		return issue("local_write_unknown")
	}
	return nil
}
