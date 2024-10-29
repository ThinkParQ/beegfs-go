//go:build beegfs

package ioctl

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/v8/common/beegfs"
)

// We cannot run these tests without an actual BeeGFS file system. The following
// constants define how that file system is expected to be configured. We could
// potentially configure this using environment variables if we need to modify
// the tests' behavior based on the test runner.
const (
	// Mount should use a file at /etc/beegfs/beegfs-client.conf.
	expectedClientPath = "/etc/beegfs/beegfs-client.conf"
	testMountPoint     = "/mnt/beegfs"
)

// Helper function to create a temporary path for testing under the provided
// path. Returns the full path that should be used for testing and a function
// that should be called (usually with defer) to cleanup after the test. Will
// fail the test if the cleanup function encounters any errors
func getTempBeeGFSPathForTesting() (string, func(t require.TestingT), error) {
	tempBeeGFSDir, err := os.MkdirTemp(testMountPoint, "ioctltests")
	if err != nil {
		return "", nil, err
	}

	cleanup := func(t require.TestingT) {
		require.NoError(t, os.RemoveAll(tempBeeGFSDir), "error cleaning up after test")
	}

	return tempBeeGFSDir + "/", cleanup, nil

}

func TestBeeGFSGetConfigFile(t *testing.T) {
	path, err := GetConfigFile(testMountPoint)
	require.NoError(t, err)
	assert.Equal(t, expectedClientPath, path)
}

// We mostly indirectly test GetEntryInfo because if it returns invalid results other tests will
// fail. Its hard to directly test because IDs will change between file systems. However
// there are a few things we could still test:
//   - All supported EntryTypes
//   - All supported FeatureFlags
func TestGetEntryInfo(t *testing.T) {
	testDir, cleanup, err := getTempBeeGFSPathForTesting()
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)

	// Test with a directory:
	dirEntryInfo, err := GetEntryInfo(testDir)
	require.NoError(t, err)
	assert.Equal(t, beegfs.EntryDirectory, dirEntryInfo.EntryType)

	// Test with a regular file:
	testPath := testDir + "file1"
	os.Create(testPath)
	fileEntryInfo, err := GetEntryInfo(testPath)
	require.NoError(t, err)
	assert.Equal(t, beegfs.EntryRegularFile, fileEntryInfo.EntryType)
}

func TestCreateFileStripeHints(t *testing.T) {
	testDir, cleanup, err := getTempBeeGFSPathForTesting()
	require.NoError(t, err, "error during test setup")
	defer cleanup(t)
	require.NoError(t, CreateFileWithStripeHints(testDir+"helloworld", 0755, 0, 0))
}
