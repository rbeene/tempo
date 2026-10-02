package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/privatefs"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"
)

type fileStore struct {
	path    string
	timeout time.Duration
	fail    func(string) error
}

func (s *fileStore) fault(stage string) error {
	if s.fail != nil {
		return s.fail(stage)
	}
	return nil
}
func (s *fileStore) location() (string, error) {
	p := s.path
	if p == "" {
		d, e := os.UserConfigDir()
		if e != nil {
			return "", failure("state_corrupt")
		}
		p = filepath.Join(d, "tempo", "activity-state.json")
	}
	if !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return "", failure("validation")
	}
	if runtime.GOOS == "darwin" {
		for _, prefix := range []string{"/var/", "/tmp/"} {
			if strings.HasPrefix(p, prefix) {
				p = "/private" + p
				break
			}
		}
	}
	return p, nil
}

// secureDirectory verifies path components, then pins the private leaf by descriptor.
// Parent permissions are never changed. Standard macOS /var and /tmp aliases are
// canonicalized before this walk; user-supplied symlinks are not followed.
func secureDirectory(dir string, create bool) (*os.Root, os.FileInfo, error) {
	r, info, err := privatefs.OpenDirectory(dir, create)
	if errors.Is(err, privatefs.ErrUnsafe) {
		err = failure("state_corrupt")
	}
	if errors.Is(err, privatefs.ErrDurability) {
		err = failure("local_write_unknown")
	}
	return r, info, err
}

type lockedStore struct {
	root        *os.Root
	lock        *os.File
	dir         string
	dirInfo     os.FileInfo
	name        string
	stateInfo   os.FileInfo
	stateExists bool
}

func (l *lockedStore) close() { unlockFile(l.lock); l.lock.Close(); l.root.Close() }
func (l *lockedStore) verifyState() error {
	current, err := l.root.Lstat(l.name)
	if !l.stateExists {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return failure("state_corrupt")
	}
	if err != nil || !privateInfo(current, false) || !os.SameFile(l.stateInfo, current) || current.Size() != l.stateInfo.Size() || !current.ModTime().Equal(l.stateInfo.ModTime()) {
		return failure("state_corrupt")
	}
	return nil
}
func (l *lockedStore) verify() error {
	fi, e := os.Lstat(l.dir)
	if e != nil || fi.Mode()&os.ModeSymlink != 0 || !os.SameFile(fi, l.dirInfo) {
		return failure("state_corrupt")
	}
	a, e := l.lock.Stat()
	if e != nil {
		return failure("state_corrupt")
	}
	b, e := l.root.Lstat(l.name + ".lock")
	if e != nil || !os.SameFile(a, b) || !privateInfo(a, false) || b.Mode()&os.ModeSymlink != 0 {
		return failure("state_corrupt")
	}
	return nil
}
func (s *fileStore) acquire(ctx context.Context, create bool) (*lockedStore, bool, error) {
	p, err := s.location()
	if err != nil {
		return nil, false, err
	}
	dir, name := filepath.Dir(p), filepath.Base(p)
	root, info, err := secureDirectory(dir, create)
	if errors.Is(err, os.ErrNotExist) && !create {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, failureFrom(err)
	}
	exists := true
	fi, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		exists = false
	} else if err != nil || !privateInfo(fi, false) {
		root.Close()
		return nil, false, failure("state_corrupt")
	}
	if !exists && !create {
		root.Close()
		return nil, false, nil
	}
	flags := os.O_RDWR
	if create && !exists {
		flags |= os.O_CREATE
	}
	lock, err := openNoFollow(root, name+".lock", flags, 0600)
	// Simultaneous first creators can observe ENOENT from openat on macOS.
	// Retry only this initial create, through the same pinned root/no-follow
	// boundary; all inode/permission checks below still apply.
	for attempt := 0; create && !exists && errors.Is(err, os.ErrNotExist) && attempt < 3; attempt++ {
		lock, err = openNoFollow(root, name+".lock", flags, 0600)
	}
	if err != nil {
		root.Close()
		return nil, false, failure("state_corrupt")
	}
	if create && !exists {
		if lock.Sync() != nil {
			lock.Close()
			root.Close()
			return nil, false, failure("local_write_unknown")
		}
	}
	li, err := lock.Stat()
	if err != nil || !privateInfo(li, false) {
		lock.Close()
		root.Close()
		return nil, false, failure("state_corrupt")
	}
	timeout := s.timeout
	if timeout == 0 {
		timeout = 250 * time.Millisecond
	}
	if timeout < 0 || timeout > time.Second {
		lock.Close()
		root.Close()
		return nil, false, failure("validation")
	}
	limit, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if limit.Err() != nil {
			lock.Close()
			root.Close()
			return nil, false, failure("state_busy")
		}
		ok, e := tryLockFile(lock)
		if e != nil {
			lock.Close()
			root.Close()
			return nil, false, failure("state_corrupt")
		}
		if ok {
			break
		}
		select {
		case <-limit.Done():
		case <-time.After(5 * time.Millisecond):
		}
	}
	l := &lockedStore{root: root, lock: lock, dir: dir, dirInfo: info, name: name}
	if err := l.verify(); err != nil {
		l.close()
		return nil, false, err
	}
	return l, true, nil
}
func failureFrom(err error) error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return failure("state_corrupt")
}
func decodeState(l *lockedStore) (*state, bool, error) {
	f, err := openNoFollow(l.root, l.name, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		l.stateExists = false
		l.stateInfo = nil
		return emptyState(), false, nil
	}
	if err != nil {
		return nil, false, failure("state_corrupt")
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !privateInfo(fi, false) || fi.Size() > maxStateBytes {
		return nil, false, failure("state_corrupt")
	}
	l.stateExists = true
	l.stateInfo = fi
	b, err := io.ReadAll(io.LimitReader(f, maxStateBytes+1))
	if err != nil || len(b) > maxStateBytes || !strictJSON(b) || !exactJSONFields(b, reflect.TypeOf(state{})) {
		return nil, false, failure("state_corrupt")
	}
	var st state
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if dec.Decode(&st) != nil || !validState(&st) {
		return nil, false, failure("state_corrupt")
	}
	return &st, true, nil
}
func (s *fileStore) read(ctx context.Context) (*state, bool, error) {
	l, ok, err := s.acquire(ctx, false)
	if err != nil || !ok {
		return emptyState(), false, err
	}
	defer l.close()
	return decodeState(l)
}
func (s *fileStore) syncVisible(l *lockedStore) error {
	if err := l.verifyState(); err != nil {
		return err
	}
	if err := l.verify(); err != nil {
		return err
	}
	f, err := openNoFollow(l.root, l.name, os.O_RDONLY, 0)
	if err != nil {
		return failure("local_write_unknown")
	}
	actual, statErr := f.Stat()
	if statErr != nil || !os.SameFile(l.stateInfo, actual) {
		f.Close()
		return failure("state_corrupt")
	}
	err = f.Sync()
	f.Close()
	if err != nil {
		return failure("local_write_unknown")
	}
	if s.fault("directory_sync") != nil {
		return failure("local_write_unknown")
	}
	d, err := l.root.Open(".")
	if err != nil {
		return failure("local_write_unknown")
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		return failure("local_write_unknown")
	}
	return nil
}
func (s *fileStore) update(ctx context.Context, fn func(*state) (bool, error)) error {
	if ctx.Err() != nil {
		return failure("state_busy")
	}
	l, _, err := s.acquire(ctx, true)
	if err != nil {
		return err
	}
	defer l.close()
	st, exists, err := decodeState(l)
	if err != nil {
		return err
	}
	changed, err := fn(st)
	if err != nil {
		return err
	}
	if !changed {
		if err := l.verify(); err != nil {
			return err
		}
		if exists {
			return s.syncVisible(l)
		}
		return l.verifyState()
	}
	if n, ok := counter(st.Revision); !ok || n == ^uint64(0) {
		return failure("validation")
	}
	st.Revision = bump(st.Revision)
	if !validState(st) {
		return failure("validation")
	}
	b, err := json.Marshal(st)
	if err != nil {
		return failure("validation")
	}
	if len(b) > maxStateBytes {
		return &Error{Code: "validation", Message: "local activity state capacity reached; preserve the existing state for review"}
	}
	if ctx.Err() != nil {
		return failure("state_busy")
	}
	if s.fault("before_write") != nil {
		return failure("state_corrupt")
	}
	tmp := ".activity-" + newID() + ".tmp"
	f, err := openNoFollow(l.root, tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return failure("state_corrupt")
	}
	defer l.root.Remove(tmp)
	_, err = f.Write(b)
	if err == nil {
		err = s.fault("file_sync")
	}
	if err == nil {
		err = f.Sync()
	}
	tempInfo, statErr := f.Stat()
	if err == nil {
		err = statErr
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return failure("state_corrupt")
	}
	if ctx.Err() != nil {
		return failure("state_busy")
	}
	if err = l.verify(); err != nil {
		return err
	}
	if s.fault("rename") != nil {
		return failure("state_corrupt")
	}
	if err := l.verifyState(); err != nil {
		return err
	}
	if l.root.Rename(tmp, l.name) != nil {
		return failure("local_write_unknown")
	}
	l.stateInfo = tempInfo
	l.stateExists = true
	if s.syncVisible(l) != nil {
		return failure("local_write_unknown")
	}
	return nil
}

// JSON's default decoder accepts duplicate keys and malformed UTF-8. Reject
// those before decoding any persisted or ingress value/fingerprint.
func strictJSON(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 64 {
			return false
		}
		t, e := d.Token()
		if e != nil {
			return false
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return false
				}
				key, ok := k.(string)
				if !ok || seen[key] {
					return false
				}
				seen[key] = true
				if !value(depth + 1) {
					return false
				}
			}
			end, e := d.Token()
			return e == nil && end == json.Delim('}')
		case '[':
			for d.More() {
				if !value(depth + 1) {
					return false
				}
			}
			end, e := d.Token()
			return e == nil && end == json.Delim(']')
		default:
			return false
		}
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}

// Reject case-insensitive field aliases throughout typed persisted records.
func exactJSONFields(raw []byte, typ reflect.Type) bool {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == reflect.TypeOf(time.Time{}) || string(raw) == "null" {
		return true
	}
	switch typ.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil || object == nil {
			return false
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			if name != "" && name != "-" {
				fields[name] = f.Type
			}
		}
		for key, value := range object {
			ft, ok := fields[key]
			if !ok || !exactJSONFields(value, ft) {
				return false
			}
		}
	case reflect.Map:
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil {
			return false
		}
		for _, v := range object {
			if !exactJSONFields(v, typ.Elem()) {
				return false
			}
		}
	case reflect.Slice:
		var array []json.RawMessage
		if json.Unmarshal(raw, &array) != nil {
			return false
		}
		for _, v := range array {
			if !exactJSONFields(v, typ.Elem()) {
				return false
			}
		}
	}
	return true
}
