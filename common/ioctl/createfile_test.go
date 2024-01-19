package ioctl

import (
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Things we don't test but probably should eventually:
//   - SetUID
//   - SetGID
//   - SetPreferredTargets
//   - SetStoragePool
//   - That we automatically detect when the parent is buddy mirrored.
//
// These are harder to test because they require the test to run as root, and the BeeGFS under test
// to be setup with multiple targets and storage pools.
func TestCreateFile(t *testing.T) {

	testDir, cleanup, err := getTempBeeGFSPathForTesting()
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	testFile := testDir + "file1"
	testSymlink := testDir + "symlink1"

	// Create regular file:
	require.NoError(t, CreateFile(testFile))

	// Create symlink:
	require.NoError(t, CreateFile(testSymlink, SetSymlinkTo(testFile), SetPermissions(0777)))

	// Check regular file was created correctly:
	statTestFile, err := os.Stat(testFile)
	require.NoError(t, err)
	assert.Equal(t, 0o0644, int(statTestFile.Mode().Perm()))
	assert.Equal(t, 0, int(statTestFile.Mode().Type()))

	// Check symbolic link was created correctly using Lstat so we don't follow the link:
	statSymlink, err := os.Lstat(testSymlink)
	require.NoError(t, err)
	assert.Equal(t, 0o0777, int(statSymlink.Mode().Perm()))
	assert.Equal(t, fs.ModeSymlink, statSymlink.Mode().Type())
}
