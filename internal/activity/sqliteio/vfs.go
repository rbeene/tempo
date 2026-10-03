//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

const vfsName = "tempo-pinned-v1"

var initializeOnce sync.Once
var initializeError error

// Registered VFS memory and top-level function values live for the process.
var vfsMemory, vfsNameMemory uintptr
var originalOpen uintptr
var callbackValues = struct {
	full      func(*libc.TLS, uintptr, uintptr, int32, uintptr) int32
	openVFS   func(*libc.TLS, uintptr, uintptr, uintptr, int32, uintptr) int32
	open      func(*libc.TLS, uintptr, int32, int32) int32
	stat      func(*libc.TLS, uintptr, uintptr) int32
	access    func(*libc.TLS, uintptr, int32) int32
	unlink    func(*libc.TLS, uintptr) int32
	directory func(*libc.TLS, uintptr, uintptr) int32
	readlink  func(*libc.TLS, uintptr, uintptr, lib.Tsize_t) lib.Tssize_t
	getcwd    func(*libc.TLS, uintptr, lib.Tsize_t) uintptr
	mkdir     func(*libc.TLS, uintptr, lib.Tmode_t) int32
	rmdir     func(*libc.TLS, uintptr) int32
}{fullPath, openVFS, openPath, statPath, accessPath, unlinkPath, openDirectory, rejectReadlink, rejectGetcwd, rejectMkdir, rejectRmdir}

func functionPointer[T any](fn T) uintptr { return *(*uintptr)(unsafe.Pointer(&fn)) }

func initialize() error {
	initializeOnce.Do(func() {
		// Preserve the stock driver's initialization hook: on Linux arm64 this
		// fixes Unix SHM page-size handling (modernc issue 199); other targets nop.
		lib.PatchIssue199()
		tls := libc.NewTLS()
		defer tls.Close()
		if rc := lib.Xsqlite3_initialize(tls); rc != lib.SQLITE_OK {
			initializeError = sqliteError(rc)
			return
		}
		// Darwin's default unix autolock finder calls statfs(path) outside the
		// syscall table. Use its registered upstream POSIX implementation.
		baseName := "unix"
		if runtime.GOOS == "darwin" {
			baseName = "unix-posix"
		}
		name, err := libc.CString(baseName)
		if err != nil {
			initializeError = err
			return
		}
		base := lib.Xsqlite3_vfs_find(tls, name)
		libc.Xfree(tls, name)
		if base == 0 {
			initializeError = ErrUnsafe
			return
		}
		original := *(*lib.Tsqlite3_vfs)(unsafe.Pointer(base))
		if original.FiVersion != 3 || original.FxSetSystemCall == 0 || original.FxGetSystemCall == 0 || original.FxOpen == 0 {
			initializeError = ErrUnsafe
			return
		}
		set := *(*func(*libc.TLS, uintptr, uintptr, uintptr) int32)(unsafe.Pointer(&original.FxSetSystemCall))
		get := *(*func(*libc.TLS, uintptr, uintptr) uintptr)(unsafe.Pointer(&original.FxGetSystemCall))
		hooks := []struct {
			name string
			ptr  uintptr
		}{
			{"open", functionPointer(callbackValues.open)},
			{"stat", functionPointer(callbackValues.stat)},
			{"lstat", functionPointer(callbackValues.stat)},
			{"access", functionPointer(callbackValues.access)},
			{"unlink", functionPointer(callbackValues.unlink)},
			{"openDirectory", functionPointer(callbackValues.directory)},
			{"readlink", functionPointer(callbackValues.readlink)},
			{"getcwd", functionPointer(callbackValues.getcwd)},
			{"mkdir", functionPointer(callbackValues.mkdir)},
			{"rmdir", functionPointer(callbackValues.rmdir)},
		}
		// Verify every required hook before changing the shared Unix table.
		for _, h := range hooks {
			p, e := libc.CString(h.name)
			if e != nil {
				initializeError = e
				return
			}
			available := get(tls, base, p) != 0
			libc.Xfree(tls, p)
			if !available {
				initializeError = ErrUnsafe
				return
			}
		}
		vfsNameMemory, err = libc.CString(vfsName)
		if err != nil {
			initializeError = err
			return
		}
		vfsMemory = lib.Xsqlite3_malloc64(tls, uint64(unsafe.Sizeof(original)))
		if vfsMemory == 0 {
			initializeError = ErrUnsafe
			return
		}
		originalOpen = original.FxOpen
		copyVFS := original
		copyVFS.FpNext = 0
		copyVFS.FzName = vfsNameMemory
		copyVFS.FxFullPathname = functionPointer(callbackValues.full)
		copyVFS.FxOpen = functionPointer(callbackValues.openVFS)
		*(*lib.Tsqlite3_vfs)(unsafe.Pointer(vfsMemory)) = copyVFS
		for _, h := range hooks {
			p, e := libc.CString(h.name)
			if e != nil {
				initializeError = e
				return
			}
			rc := set(tls, base, p, h.ptr)
			libc.Xfree(tls, p)
			if rc != lib.SQLITE_OK {
				initializeError = sqliteError(rc)
				return
			}
		}
		if rc := lib.Xsqlite3_vfs_register(tls, vfsMemory, 0); rc != lib.SQLITE_OK {
			initializeError = sqliteError(rc)
		}
	})
	return initializeError
}

func setError(tls *libc.TLS, err error) int32 {
	code := int32(unix.EIO)
	var errno syscall.Errno
	if errors.As(err, &errno) {
		code = int32(errno)
	}
	*errnoPointer(tls) = code
	return -1
}
func fullPath(tls *libc.TLS, _ uintptr, input uintptr, n int32, output uintptr) int32 {
	path := libc.GoString(input)
	r, name, err := lookupPath(path)
	if err != nil || name != r.databaseName || strings.ContainsAny(path, "?\x00") || len(path)+1 > int(n) {
		return lib.SQLITE_CANTOPEN
	}
	copy(unsafe.Slice((*byte)(unsafe.Pointer(output)), int(n)), append([]byte(path), 0))
	emit(pathEvent(r, name, "fullpath", "preserved", 0, -1, nil))
	return lib.SQLITE_OK
}
func openVFS(tls *libc.TLS, vfs, name, file uintptr, flags int32, out uintptr) int32 {
	if name == 0 {
		return lib.SQLITE_CANTOPEN
	}
	r, base, err := lookupPath(libc.GoString(name))
	if err != nil {
		return lib.SQLITE_CANTOPEN
	}
	allowed := int32(lib.SQLITE_OPEN_MAIN_DB)
	if base == r.databaseName+"-wal" {
		allowed = lib.SQLITE_OPEN_WAL
	}
	if base == r.databaseName+"-journal" {
		allowed = lib.SQLITE_OPEN_MAIN_JOURNAL
	}
	if base == r.databaseName+"-shm" || flags&allowed == 0 {
		return lib.SQLITE_CANTOPEN
	}
	if refuseInitialSidecar(r, base, "vfs-open") {
		return lib.SQLITE_CANTOPEN
	}
	emit(pathEvent(r, base, "vfs-open", "role", int(flags), -1, nil))
	fn := *(*func(*libc.TLS, uintptr, uintptr, uintptr, int32, uintptr) int32)(unsafe.Pointer(&originalOpen))
	rc := fn(tls, vfs, name, file, flags, out)
	emit(event{Namespace: r.key, Role: role(r, base), Op: "vfs-open", Phase: "returned", Flags: int(flags), FD: -1, Code: rc})
	return rc
}

func aliasLocked(id identity, owner *rootEntry, name string) bool {
	for _, other := range registry.roots {
		if !other.active {
			continue
		}
		if other.guardID == id && id != (identity{}) && (other != owner || name != other.guardName) {
			return true
		}
		for otherName, known := range other.known {
			if known == id && (other != owner || otherName != name) {
				return true
			}
		}
	}
	return false
}

func openPath(tls *libc.TLS, p uintptr, rawFlags, _ int32) int32 {
	path, flags := libc.GoString(p), int(rawFlags)
	if path == "/dev/urandom" || path == "/dev/null" {
		if flags&^(unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW) != 0 {
			return setError(tls, ErrUnsafe)
		}
		fd, err := unix.Open(path, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		emit(event{Namespace: "system", Role: path, Op: "open", Phase: "system-read-only", Flags: flags, FD: fd})
		if err != nil {
			return setError(tls, err)
		}
		return int32(fd)
	}
	r, name, err := lookupPath(path)
	if err != nil {
		return setError(tls, err)
	}
	if refuseInitialSidecar(r, name, "open") {
		return setError(tls, ErrUnsafe)
	}
	if flags&unix.O_TRUNC != 0 || flags&unix.O_CREAT != 0 && name == r.databaseName && !r.create {
		return setError(tls, ErrUnsafe)
	}
	if name == r.databaseName && r.exclusive {
		if err = exclusiveAttempt(r, flags); err != nil {
			return setError(tls, err)
		}
		flags |= unix.O_RDWR | unix.O_CREAT | unix.O_EXCL
	}
	if err = preflight(r, name, flags&unix.O_CREAT != 0); err != nil {
		refuseExclusive(r, name, err)
		emit(pathEvent(r, name, "open", "preflight-refused", flags, -1, err))
		return setError(tls, err)
	}
	emit(pathEvent(r, name, "open", "before", flags, -1, nil))
	var injected error
	if name == r.databaseName && r.exclusive {
		injected = sqlEvent(sqlTestEvent{Phase: "exclusive-open-before", Operation: "exclusive-open"})
		if injected != nil && !errors.Is(injected, unix.EINTR) && !errors.Is(injected, unix.EIO) {
			injected = unix.EIO
		}
	}
	registry.Lock()
	if registry.poisoned || registry.fatal || r.poisoned {
		registry.Unlock()
		return setError(tls, ErrUnsafe)
	}
	// Keep admission/rejection serialized across stores. A rejected FD must not
	// close while it could alias an inode with this process's live POSIX locks.
	flags |= unix.O_NOFOLLOW | unix.O_CLOEXEC
	fd := -1
	if name == r.databaseName && r.exclusive {
		if err = admissionError(r.openContext, r.openDeadline); err != nil || r.exclusiveRefused || r.exclusiveCreated {
			injected = unix.EIO
		}
		if injected == nil {
			injected = noSidecars(r)
		}
	}
	err = injected
	if err == nil {
		fd, err = unix.Openat(r.fd, name, flags, 0600)
	}
	if err != nil {
		if name == r.databaseName && r.exclusive && !errors.Is(err, unix.EINTR) {
			r.exclusiveRefused = true
		}
		registry.Unlock()
		emit(pathEvent(r, name, "open", "after", flags, -1, err))
		if name == r.databaseName && r.exclusive && !errors.Is(err, unix.EINTR) {
			emit(pathEvent(r, name, "exclusive", "refused", flags, -1, err))
		}
		return setError(tls, err)
	}
	var actual, named unix.Stat_t
	e1 := unix.Fstat(fd, &actual)
	e2 := unix.Fstatat(r.fd, name, &named, unix.AT_SYMLINK_NOFOLLOW)
	id := fileIdentity(&actual)
	expected := r.known[name]
	bad := e1 != nil || e2 != nil || !privateFile(&actual) || !privateFile(&named) || id != fileIdentity(&named) || expected != (identity{}) && id != expected || aliasLocked(id, r, name)
	if name == r.databaseName && r.exclusive && noSidecars(r) != nil {
		bad = true
	}
	if bad {
		registry.rejected = append(registry.rejected, fd)
		registry.poisoned, r.poisoned = true, true
		if name == r.databaseName && r.exclusive {
			r.exclusiveRefused = true
		}
		registry.Unlock()
		emit(pathEvent(r, name, "open", "quarantined", flags, fd, ErrUnsafe))
		if name == r.databaseName && r.exclusive {
			emit(pathEvent(r, name, "exclusive", "refused", flags, fd, ErrUnsafe))
		}
		return setError(tls, ErrUnsafe)
	}
	r.known[name] = id
	if name == r.databaseName {
		r.main = id
		if r.exclusive {
			r.exclusiveCreated = true
		}
	}
	registry.Unlock()
	emit(pathEvent(r, name, "open", "validated", flags, fd, nil))
	if name == r.databaseName && r.exclusive {
		emit(pathEvent(r, name, "exclusive", "created", flags, fd, nil))
	}
	return int32(fd)
}

func statPath(tls *libc.TLS, p, output uintptr) int32 {
	r, name, err := lookupPath(libc.GoString(p))
	if err != nil {
		return setError(tls, err)
	}
	if err = preflight(r, name, true); err != nil {
		refuseExclusive(r, name, err)
		emit(pathEvent(r, name, "stat", "preflight-refused", 0, -1, err))
		return setError(tls, err)
	}
	emit(pathEvent(r, name, "stat", "before", 0, -1, nil))
	base, err := libc.CString(name)
	if err != nil {
		return setError(tls, err)
	}
	defer libc.Xfree(tls, base)
	rc := libc.Xfstatat(tls, int32(r.fd), base, output, int32(unix.AT_SYMLINK_NOFOLLOW))
	if rc != 0 {
		emit(pathEvent(r, name, "stat", "after", 0, -1, syscall.Errno(*errnoPointer(tls))))
		return rc
	}
	st := (*lib.Tstat)(unsafe.Pointer(output))
	id := identity{uint64(st.Fst_dev), uint64(st.Fst_ino)}
	registry.Lock()
	expected := r.known[name]
	bad := registry.poisoned || registry.fatal || r.poisoned || aliasLocked(id, r, name)
	exclusiveBad := name == r.databaseName && r.exclusive && (!r.exclusiveCreated || r.exclusiveRefused)
	if exclusiveBad {
		r.exclusiveRefused = true
	}
	registry.Unlock()
	if exclusiveBad {
		emit(pathEvent(r, name, "exclusive", "refused", 0, -1, ErrUnsafe))
		return setError(tls, ErrUnsafe)
	}
	if bad || uint64(st.Fst_mode)&unix.S_IFMT != unix.S_IFREG || uint64(st.Fst_mode)&0777 != 0600 || uint64(st.Fst_uid) != uint64(unix.Geteuid()) || st.Fst_nlink != 1 || expected != (identity{}) && id != expected {
		return setError(tls, ErrUnsafe)
	}
	emit(pathEvent(r, name, "stat", "after", 0, -1, nil))
	return 0
}
func accessPath(tls *libc.TLS, p uintptr, mode int32) int32 {
	r, name, err := lookupPath(libc.GoString(p))
	if err != nil {
		return setError(tls, err)
	}
	if err = preflight(r, name, false); err != nil {
		emit(pathEvent(r, name, "access", "preflight-refused", int(mode), -1, err))
		return setError(tls, err)
	}
	emit(pathEvent(r, name, "access", "before", int(mode), -1, nil))
	base, err := libc.CString(name)
	if err != nil {
		return setError(tls, err)
	}
	defer libc.Xfree(tls, base)
	rc := libc.Xfaccessat(tls, int32(r.fd), base, mode, int32(unix.AT_SYMLINK_NOFOLLOW))
	var resultError error
	if rc != 0 {
		resultError = syscall.Errno(*errnoPointer(tls))
	}
	emit(pathEvent(r, name, "access", "after", int(mode), -1, resultError))
	return rc
}
func unlinkPath(tls *libc.TLS, p uintptr) int32 {
	r, name, err := lookupPath(libc.GoString(p))
	if err != nil || name == r.databaseName {
		return setError(tls, ErrUnsafe)
	}
	if refuseInitialSidecar(r, name, "unlink") {
		return setError(tls, ErrUnsafe)
	}
	if err = preflight(r, name, false); err != nil {
		return setError(tls, err)
	}
	emit(pathEvent(r, name, "unlink", "before", 0, -1, nil))
	if err = preflight(r, name, false); err != nil {
		return setError(tls, err)
	}
	// A swapped final symlink is unlinked, never followed; only the pinned
	// directory entry can change. Its outside target is never accessed.
	if err = unix.Unlinkat(r.fd, name, 0); err != nil {
		return setError(tls, err)
	}
	registry.Lock()
	delete(r.known, name)
	registry.Unlock()
	emit(pathEvent(r, name, "unlink", "after", 0, -1, nil))
	return 0
}
func openDirectory(tls *libc.TLS, p, output uintptr) int32 {
	*(*int32)(unsafe.Pointer(output)) = -1
	r, name, err := lookupPath(libc.GoString(p))
	if err != nil {
		setError(tls, err)
		return lib.SQLITE_CANTOPEN
	}
	emit(pathEvent(r, name, "directory", "before", 0, -1, nil))
	var st unix.Stat_t
	if unix.Fstat(r.fd, &st) != nil || !privateDirectory(&st) || fileIdentity(&st) != r.id {
		setError(tls, ErrUnsafe)
		return lib.SQLITE_CANTOPEN
	}
	fd, err := unix.FcntlInt(uintptr(r.fd), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		setError(tls, err)
		return lib.SQLITE_CANTOPEN
	}
	*(*int32)(unsafe.Pointer(output)) = int32(fd)
	emit(pathEvent(r, name, "directory", "validated", 0, fd, nil))
	return lib.SQLITE_OK
}
func rejectReadlink(tls *libc.TLS, _, _ uintptr, _ lib.Tsize_t) lib.Tssize_t {
	setError(tls, unix.ELOOP)
	return -1
}
func rejectGetcwd(tls *libc.TLS, _ uintptr, _ lib.Tsize_t) uintptr {
	setError(tls, unix.EACCES)
	return 0
}
func rejectMkdir(tls *libc.TLS, _ uintptr, _ lib.Tmode_t) int32 { return setError(tls, unix.EACCES) }
func rejectRmdir(tls *libc.TLS, _ uintptr) int32                { return setError(tls, unix.EACCES) }

// fullPathForTest calls the installed callback without bypassing its parser.
func fullPathForTest(input string, outputBytes int32) (string, int32) {
	tls := libc.NewTLS()
	defer tls.Close()
	p, e := libc.CString(input)
	if e != nil {
		return "", lib.SQLITE_NOMEM
	}
	defer libc.Xfree(tls, p)
	if outputBytes <= 0 {
		return "", lib.SQLITE_CANTOPEN
	}
	out := lib.Xsqlite3_malloc64(tls, uint64(outputBytes))
	if out == 0 {
		return "", lib.SQLITE_NOMEM
	}
	defer lib.Xsqlite3_free(tls, out)
	rc := fullPath(tls, 0, p, outputBytes, out)
	if rc != lib.SQLITE_OK {
		return "", rc
	}
	return libc.GoString(out), rc
}
