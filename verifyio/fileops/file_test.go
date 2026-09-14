// This is a unit test.
//
// Coverage: Open's O_NOFOLLOW symlink refusal, with and without O_CREATE (and
// that it doesn't regress the ordinary reopen/truncate case); Open's prompt --
// not merely eventual -- refusal of a non-regular target, FIFO and directory;
// IOType.String's known
// values plus its unrecognized-value fallback; Write/ReadAt round-tripping
// via IOTypeBuffered; Write rejecting the not-yet-implemented IOTypes with
// ErrIOTypeNotSupported; Sync's happy path; LockFd returning the same
// underlying file used for Read/Write, not a separate handle; and bad-input
// handling that fileops itself doesn't validate but passes through to
// os.File -- a negative offset, a read past EOF, and a zero-length buffer
// (valid no-op, even past EOF).
package fileops

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestOpenRefusesToFollowSymlinkOnCreate is a regression test for a local
// symlink attack: several callers of Open pass a fixed, predictable default
// path (e.g. /tmp/iotest-smoke.dat). Without O_NOFOLLOW, a local attacker
// could pre-plant a symlink at that path pointing at a victim-writable file
// elsewhere, and Open's O_CREATE|O_TRUNC would transparently follow it and
// clobber the victim's file. Open must refuse instead of following the link.
func TestOpenRefusesToFollowSymlinkOnCreate(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	const original = "original victim content"
	if err := os.WriteFile(victim, []byte(original), 0644); err != nil {
		t.Fatalf("WriteFile(victim): %v", err)
	}
	link := filepath.Join(dir, "planted-link")
	if err := os.Symlink(victim, link); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	f, err := Open(link, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err == nil {
		f.Close()
		t.Fatal("Open(symlink, O_CREATE): got nil error, want a refusal to follow the symlink")
	}
	if !errors.Is(err, syscall.ELOOP) {
		t.Errorf("Open(symlink, O_CREATE) failed, but not with ELOOP as O_NOFOLLOW should produce: %v", err)
	}

	data, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("ReadFile(victim) after rejected Open: %v", err)
	}
	if string(data) != original {
		t.Fatalf("victim file was modified: got %q, want %q", data, original)
	}
}

// TestOpenRefusesToFollowSymlinkWithoutCreate is the other half of the guard
// above, and the case it used to miss: O_NOFOLLOW was added only when the
// caller passed O_CREATE, but a pre-planted symlink means the final component
// already exists, so O_CREATE is the one flag the attack does not need. Note
// os.O_RDONLY is 0, so the read-only open -- what iotest-verify passes --
// carried no guard bit at all and followed the link to whatever it pointed at.
func TestOpenRefusesToFollowSymlinkWithoutCreate(t *testing.T) {
	const original = "original victim content"
	for _, tc := range []struct {
		name  string
		flags int
	}{
		{"O_RDONLY", os.O_RDONLY},
		{"O_RDWR", os.O_RDWR},
		{"O_RDWR|O_TRUNC", os.O_RDWR | os.O_TRUNC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			victim := filepath.Join(dir, "victim")
			if err := os.WriteFile(victim, []byte(original), 0644); err != nil {
				t.Fatalf("WriteFile(victim): %v", err)
			}
			link := filepath.Join(dir, "planted-link")
			if err := os.Symlink(victim, link); err != nil {
				t.Fatalf("Symlink: %v", err)
			}

			f, err := Open(link, tc.flags, 0644)
			if err == nil {
				f.Close()
				t.Fatal("Open(symlink): got nil error, want a refusal to follow the symlink")
			}
			if !errors.Is(err, syscall.ELOOP) {
				t.Errorf("Open(symlink) failed, but not with ELOOP as O_NOFOLLOW should produce: %v", err)
			}
			data, err := os.ReadFile(victim)
			if err != nil {
				t.Fatalf("ReadFile(victim) after rejected Open: %v", err)
			}
			if string(data) != original {
				t.Errorf("victim file was modified: got %q, want %q", data, original)
			}
		})
	}
}

// TestOpenRefusesNonRegularFile pins both halves of the non-regular rejection:
// that it happens at all, and that it happens *promptly*.
//
// The FIFO case is why Open passes O_NONBLOCK. open(2) on a FIFO O_RDONLY blocks
// indefinitely waiting for a writer, so an fstat check alone would never run --
// aiming a tool at a FIFO wedged it in open with nothing printed. That needs no
// attacker: a mistyped -path is enough. The test therefore fails on a timeout
// rather than hanging the package, since a regression here is a hang, not a
// wrong value.
func TestOpenRefusesNonRegularFile(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "planted-fifo")
	if err := syscall.Mkfifo(fifo, 0666); err != nil {
		t.Skipf("mkfifo unsupported on this filesystem: %v", err)
	}

	for _, tc := range []struct{ name, path string }{
		{"fifo", fifo},
		{"directory", dir},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type result struct {
				f   *File
				err error
			}
			done := make(chan result, 1)
			go func() {
				f, err := Open(tc.path, os.O_RDONLY, 0)
				done <- result{f, err}
			}()

			select {
			case got := <-done:
				if got.err == nil {
					got.f.Close()
					t.Fatalf("Open(%s): got nil error, want a refusal to open a non-regular file", tc.name)
				}
				if !strings.Contains(got.err.Error(), "not a regular file") {
					t.Errorf("Open(%s): err=%v, want it to say 'not a regular file'", tc.name, got.err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("Open(%s) did not return within 10s -- it must not block on a "+
					"non-regular file (O_NONBLOCK is what keeps open(2) from waiting on a writer)", tc.name)
			}
		})
	}
}

// TestOpenDoesNotSetNonblock pins the flag OFF. O_NONBLOCK is not inert on
// BeeGFS: it maps to OPENFILE_ACCESS_NONBLOCKING, which suppresses the
// client's open retry, so a file under a transient state lock fails
// EWOULDBLOCK instead of being retried into success -- a FAIL on a healthy
// file. It cannot be cleared after the fact; the flag travels on the
// OpenFileMsg.
func TestOpenDoesNotSetNonblock(t *testing.T) {
	dir := t.TempDir()
	f, err := Open(filepath.Join(dir, "data.dat"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	fl, err := unix.FcntlInt(f.LockFd().Fd(), unix.F_GETFL, 0)
	if err != nil {
		t.Fatalf("F_GETFL: %v", err)
	}
	if fl&unix.O_NONBLOCK != 0 {
		t.Errorf("F_GETFL=%#o has O_NONBLOCK set, want it absent", fl)
	}
}

// TestOpenAllowsCreateWithoutSymlink is a sanity check that O_NOFOLLOW
// doesn't regress the ordinary case: reopening/truncating a real (non-symlink)
// file across runs, which every caller of Open with O_CREATE relies on.
func TestOpenAllowsCreateWithoutSymlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.dat")
	if err := os.WriteFile(path, []byte("first run"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	f, err := Open(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("Open(regular file, O_CREATE): %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestIOTypeString(t *testing.T) {
	cases := []struct {
		ioType IOType
		want   string
	}{
		{IOTypeBuffered, "buffered"},
		{IOTypeODirect, "odirect"},
		{IOTypeMmap, "mmap"},
		{IOTypePwritev, "pwritev"},
		{IOType(99), "iotype(99)"},
	}
	for _, c := range cases {
		if got := c.ioType.String(); got != c.want {
			t.Errorf("IOType(%d).String() = %q, want %q", int(c.ioType), got, c.want)
		}
	}
}

func TestWriteReadAtRoundTrip(t *testing.T) {
	dir := t.TempDir()
	f, err := Open(filepath.Join(dir, "data.dat"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	want := []byte("some block payload")
	if err := f.Write(IOTypeBuffered, want, 128); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := f.ReadAt(got, 128); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("ReadAt = %q, want %q", got, want)
	}
}

func TestWriteUnsupportedIOTypes(t *testing.T) {
	dir := t.TempDir()
	f, err := Open(filepath.Join(dir, "data.dat"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	for _, ioType := range []IOType{IOTypeODirect, IOTypeMmap, IOTypePwritev} {
		if err := f.Write(ioType, []byte("x"), 0); !errors.Is(err, ErrIOTypeNotSupported) {
			t.Errorf("Write(%s): err=%v, want ErrIOTypeNotSupported", ioType, err)
		}
	}
}

func TestSync(t *testing.T) {
	dir := t.TempDir()
	f, err := Open(filepath.Join(dir, "data.dat"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	if err := f.Write(IOTypeBuffered, []byte("data"), 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}

// TestLockFdReturnsUsableSameFile confirms LockFd returns the same
// underlying file description Write/ReadAt operate on, not a separate
// handle that could silently drift out of sync with it.
func TestLockFdReturnsUsableSameFile(t *testing.T) {
	dir := t.TempDir()
	f, err := Open(filepath.Join(dir, "data.dat"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	if _, err := f.LockFd().WriteAt([]byte("via-lockfd"), 0); err != nil {
		t.Fatalf("WriteAt via LockFd: %v", err)
	}
	got := make([]byte, len("via-lockfd"))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(got) != "via-lockfd" {
		t.Errorf("ReadAt after writing via LockFd = %q, want %q", got, "via-lockfd")
	}
}

// fileops itself does not validate offset/buffer arguments -- it passes them
// straight through to the underlying os.File. These tests pin what that
// pass-through actually does today, so a future change (e.g. adding fileops'
// own validation) can't silently alter this behavior unnoticed.

func TestReadAtNegativeOffsetReturnsError(t *testing.T) {
	dir := t.TempDir()
	f, err := Open(filepath.Join(dir, "data.dat"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	_, err = f.ReadAt(make([]byte, 4), -1)
	if err == nil {
		t.Fatal("ReadAt(-1): expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "negative offset") {
		t.Errorf("ReadAt(-1): err=%v, want it to mention 'negative offset'", err)
	}
	if !strings.Contains(err.Error(), "fileops.ReadAt") {
		t.Errorf("ReadAt(-1): err=%v, want it wrapped with the fileops.ReadAt prefix", err)
	}
}

func TestWriteNegativeOffsetReturnsError(t *testing.T) {
	dir := t.TempDir()
	f, err := Open(filepath.Join(dir, "data.dat"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	err = f.Write(IOTypeBuffered, []byte("x"), -1)
	if err == nil {
		t.Fatal("Write(-1): expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "negative offset") {
		t.Errorf("Write(-1): err=%v, want it to mention 'negative offset'", err)
	}
}

func TestReadAtPastEOFReturnsIOEOF(t *testing.T) {
	dir := t.TempDir()
	f, err := Open(filepath.Join(dir, "data.dat"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	_, err = f.ReadAt(make([]byte, 4), 0) // empty file
	if !errors.Is(err, io.EOF) {
		t.Errorf("ReadAt past EOF: err=%v, want it to wrap io.EOF", err)
	}
}

func TestReadAtWriteAtZeroLengthBufferSucceeds(t *testing.T) {
	dir := t.TempDir()
	f, err := Open(filepath.Join(dir, "data.dat"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// Even past EOF on an empty file: a zero-length request is a valid no-op,
	// not an error, per os.File's own contract.
	if err := f.Write(IOTypeBuffered, []byte{}, 5); err != nil {
		t.Errorf("Write(empty buf): %v, want nil", err)
	}
	if _, err := f.ReadAt([]byte{}, 5); err != nil {
		t.Errorf("ReadAt(empty buf): %v, want nil", err)
	}
}
