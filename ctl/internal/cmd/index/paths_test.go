package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathWithin(t *testing.T) {
	ok, err := pathWithin("/mnt/beegfs", "/mnt/beegfs/demo")
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = pathWithin("/mnt/beegfs", "/mnt/other/demo")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestReadGUFIConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gufi.conf")
	contents := "IndexRoot=/index-root\n"
	require.NoError(t, os.WriteFile(path, []byte(contents), 0600))

	values, err := readGUFIConfigFromFile(path)
	require.NoError(t, err)
	assert.Equal(t, "/index-root", values["IndexRoot"])
}

func TestResolveIndexPath_IndexRootPassthrough(t *testing.T) {
	setIndexRootForTest(t, "/index-root")

	resolved, err := resolveIndexPath("/index-root/dir")
	require.NoError(t, err)
	assert.Equal(t, "/index-root/dir", resolved)
}

func setIndexRootForTest(t *testing.T, root string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "gufi.conf")
	contents := "IndexRoot=" + root + "\n"
	require.NoError(t, os.WriteFile(path, []byte(contents), 0600))

	previous := indexConfig
	indexConfig = path
	resetGUFIConfigCache()
	t.Cleanup(func() {
		indexConfig = previous
		resetGUFIConfigCache()
	})
}

func resetGUFIConfigCache() {
	gufiConfigCache.mu.Lock()
	defer gufiConfigCache.mu.Unlock()
	gufiConfigCache.loaded = false
	gufiConfigCache.path = ""
	gufiConfigCache.values = nil
	gufiConfigCache.err = nil
}
