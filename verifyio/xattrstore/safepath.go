package xattrstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/thinkparq/beegfs-go/verifyio/xattr"
)

// CheckSafeToDestroy reports whether path looks enough like a verifyio test
// artifact to be destroyed -- truncated to zero, or deleted and recreated.
//
// Every verifyio writing tool takes an operator-supplied path and clears it
// before use, and each is normally run as root on a test box, so a slip like
// `-path /etc/passwd` is a single character away from destroying a real file.
// None of the tools can distinguish that from an intended run on their own,
// because to them one absolute path looks like any other.
//
// Deliberately NOT the same rule as the CLI's --fresh guard, which blocklists
// system *directories*. That does not help here: the hazard is a single regular
// file, and /etc/passwd is not in any directory blocklist. The rule instead is
// "it must look like something verifyio made":
//
//   - a path that does not exist is fine; it is about to be created
//   - an existing path must be a regular file, and must either already carry a
//     verifyio record xattr (so some verifyio tool produced it) or be named with
//     the iotest- prefix this tool family uses
//
// That admits every legitimate case -- a first run, a re-run, a default path
// (`/tmp/iotest-smoke.dat`), and a data file copied elsewhere with
// `cp --preserve=xattr` for post-mortem work -- while refusing an arbitrary file
// the operator almost certainly did not mean to destroy.
//
// This is a FOOTGUN guard, not a security boundary. It resolves path twice (here,
// and again when the caller opens it), so a hostile local user could swap the
// target in between. That is out of scope by design: verifyio runs as root on
// test systems, where such a user could destroy the file directly anyway. The
// failure this exists to prevent is a typo, and against a typo a path check is
// exactly the right shape.
//
// Lives here rather than in whichever tool needed it first because the question
// it answers -- "is this one of ours?" -- is about this package's xattr
// namespace, which is why XAttrPrefix is exported at all.
func CheckSafeToDestroy(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil // nothing to destroy
	}
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing to destroy %s: not a regular file (mode=%s)", path, info.Mode())
	}
	if strings.HasPrefix(filepath.Base(path), "iotest-") {
		return nil
	}
	names, err := xattr.List(path)
	if err != nil {
		return fmt.Errorf("refusing to destroy %s: cannot list its xattrs to confirm it is a "+
			"verifyio test file: %w", path, err)
	}
	for _, n := range names {
		if strings.HasPrefix(n, XAttrPrefix) {
			return nil // a previous verifyio run's file
		}
	}
	return fmt.Errorf("refusing to destroy %s: it carries no %s* xattrs and is not named "+
		"iotest-*, so it does not look like a verifyio test file. This operation clears the "+
		"file at the given path; point it at a dedicated test path", path, XAttrPrefix)
}
