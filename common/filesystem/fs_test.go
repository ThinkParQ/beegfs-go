package filesystem

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	baseTestDir  = "/tmp/"
	testFileName = "foo"
)

// Helper function to create a temporary path for testing under the provided
// path. Returns the full path that should be used for testing and a function
// that should be called (usually with defer) to cleanup after the test. Will
// fail the test if the cleanup function encounters any errors
func tempPathForTesting(path string) (string, func(t require.TestingT), error) {
	tempPathForTesting, err := os.MkdirTemp(path, "gobeeFSTesting")
	if err != nil {
		return "", nil, err
	}

	cleanup := func(t require.TestingT) {
		require.NoError(t, os.RemoveAll(tempPathForTesting), "error cleaning up after test")
	}

	return tempPathForTesting, cleanup, nil

}

func TestCreatePreallocatedFile(t *testing.T) {
	testDir, cleanup, err := tempPathForTesting(baseTestDir)
	require.NoError(t, err)
	defer cleanup(t)
	mount, err := NewFromMountPoint(testDir)
	require.NoError(t, err)
	assert.NoError(t, mount.CreatePreallocatedFile(testFileName, 2<<10, false))
}

func TestWriteAndReadFileParts(t *testing.T) {
	const (
		expectedFileLen = 37
	)
	testDir, cleanup, err := tempPathForTesting(baseTestDir)
	require.NoError(t, err)
	defer cleanup(t)
	mount, err := NewFromMountPoint(testDir)
	require.NoError(t, err)
	err = mount.CreatePreallocatedFile(testFileName, expectedFileLen, false)
	require.NoError(t, err)
	// The expected base64 encoded SHA256 hash of the resulting file.
	expectedSha256Sum := "prE1tnmy/L+UhGmlamLhJ+wG+M9v4/Q02eYe/geD2Qw="
	parts := []struct {
		start, stop int64
		data        byte
	}{
		{0, 9, 'a'},
		{10, 19, 'b'},
		{20, 29, 'c'},
		{30, 35, 'd'},
		{36, 36, '\n'},
	}

	for _, part := range parts {
		filePart, err := mount.WriteFilePart(testFileName, part.start, part.stop)
		require.NoError(t, err)
		data := make([]byte, part.stop-part.start+1)
		for i := range data {
			data[i] = part.data
		}
		bytesWritten, err := filePart.Write(data)
		assert.NoError(t, filePart.Close())
		require.NoError(t, err)
		assert.Equal(t, part.stop-part.start+1, int64(bytesWritten))
	}

	filePart, actualSha256Sum, err := mount.ReadFilePart(testFileName, 0, 36)
	require.NoError(t, err)
	assert.Equal(t, expectedSha256Sum, actualSha256Sum)

	buf := make([]byte, expectedFileLen)
	readBytes, err := filePart.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, expectedFileLen, readBytes)
}
