// This is a unit test.
//
// Coverage: CheckSafeToDestroy, the shared destructive-op guard. Every verifyio
// writing tool clears the operator-supplied file at -path, and each normally runs
// as root on a test box, so a slip like `-path /etc/passwd` is one character away
// from destroying a real file. The guard must refuse that while admitting every
// legitimate case: a first run, a re-run, a default iotest-* path, and a data file
// copied elsewhere with cp --preserve=xattr.
package xattrstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/internal/testxattr"
	"github.com/thinkparq/beegfs-go/verifyio/xattr"
)

func TestCheckSafeToDestroy(t *testing.T) {
	dir := t.TempDir()

	write := func(name, content string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return p
	}

	t.Run("nonexistent path is allowed", func(t *testing.T) {
		if err := CheckSafeToDestroy(filepath.Join(dir, "not-there.dat")); err != nil {
			t.Errorf("got %v, want nil -- nothing to destroy", err)
		}
	})

	t.Run("iotest-prefixed name is allowed", func(t *testing.T) {
		p := write("iotest-cap.dat", "")
		if err := CheckSafeToDestroy(p); err != nil {
			t.Errorf("got %v, want nil", err)
		}
	})

	t.Run("arbitrary regular file is refused", func(t *testing.T) {
		p := write("passwd-lookalike", "root:x:0:0:root:/root:/bin/bash\n")
		err := CheckSafeToDestroy(p)
		if err == nil {
			t.Fatal("want a refusal for a file that is not a verifyio artifact")
		}
		// The message has to tell the operator what to do instead.
		if !strings.Contains(err.Error(), "refusing") ||
			!strings.Contains(err.Error(), "dedicated test path") {
			t.Errorf("unhelpful refusal: %v", err)
		}
		// And the guard must not have touched it.
		if b, rerr := os.ReadFile(p); rerr != nil || len(b) == 0 {
			t.Errorf("guard damaged the file it refused: err=%v len=%d", rerr, len(b))
		}
	})

	t.Run("a directory is refused", func(t *testing.T) {
		if err := CheckSafeToDestroy(dir); err == nil {
			t.Error("want a refusal for a non-regular file")
		}
	})

	t.Run("non-iotest name carrying verifyio xattrs is allowed", func(t *testing.T) {
		// The realistic case: a data file copied elsewhere with
		// `cp --preserve=xattr` for post-mortem work, then re-probed.
		p := write("mydata.dat", "")
		testxattr.RequireSupport(t, p)
		if err := xattr.Set(p, XAttrPrefix+"0-1024", make([]byte, 4), 0); err != nil {
			t.Fatalf("xattr.Set: %v", err)
		}
		if err := CheckSafeToDestroy(p); err != nil {
			t.Errorf("got %v, want nil -- it carries a verifyio record", err)
		}
	})
}
