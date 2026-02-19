package index

import (
	"os"
	"path/filepath"
	"strings"
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

func TestResolveIndexPath_AbsoluteOutsideIndexRootErrors(t *testing.T) {
	setIndexRootForTest(t, "/index-root")

	_, err := resolveIndexPath("/other-root/dir")
	require.Error(t, err)
}

func TestResolveIndexRootOverride(t *testing.T) {
	previousOverride := indexRoot
	indexRoot = "/override-root"
	t.Cleanup(func() {
		indexRoot = previousOverride
	})

	root, ok, err := resolveIndexRoot(true)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "/override-root", root)
}

func TestResolveIndexRootOverrideRequiresAbsolute(t *testing.T) {
	previousOverride := indexRoot
	indexRoot = "relative-root"
	t.Cleanup(func() {
		indexRoot = previousOverride
	})

	_, _, err := resolveIndexRoot(true)
	assert.Error(t, err)
}

func setIndexRootForTest(t *testing.T, root string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "gufi.conf")
	contents := "IndexRoot=" + root + "\n"
	require.NoError(t, os.WriteFile(path, []byte(contents), 0600))

	previous := indexConfig
	previousOverride := indexRoot
	indexConfig = path
	indexRoot = ""
	resetGUFIConfigCache()
	t.Cleanup(func() {
		indexConfig = previous
		indexRoot = previousOverride
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

type testBeeGFSClient struct {
	mountPath string
}

func (c testBeeGFSClient) GetMountPath() string {
	return c.mountPath
}

func (c testBeeGFSClient) GetRelativePathWithinMount(path string) (string, error) {
	cleaned := filepath.Clean(path)
	if c.mountPath != "" {
		cleaned = strings.TrimPrefix(cleaned, c.mountPath)
	}
	cleaned = strings.TrimPrefix(cleaned, string(filepath.Separator))
	return filepath.Clean(string(filepath.Separator) + cleaned), nil
}

func TestResolveBeeGFSRelativePathWithClient_IndexRoot(t *testing.T) {
	client := testBeeGFSClient{mountPath: "/mnt/beegfs"}

	relative, err := resolveBeeGFSRelativePathWithClient("/mnt/index/demo/all", "/mnt/index", client)
	require.NoError(t, err)
	assert.Equal(t, "demo/all", relative)
}

func TestResolveBeeGFSRelativePathWithClient_NoIndexRoot(t *testing.T) {
	client := testBeeGFSClient{mountPath: "/mnt/beegfs"}

	relative, err := resolveBeeGFSRelativePathWithClient("/mnt/beegfs/demo/all", "", client)
	require.NoError(t, err)
	assert.Equal(t, "demo/all", relative)
}

func TestResolveBeeGFSRelativePathWithClient_Root(t *testing.T) {
	client := testBeeGFSClient{mountPath: "/mnt/beegfs"}

	relative, err := resolveBeeGFSRelativePathWithClient("/mnt/beegfs", "", client)
	require.NoError(t, err)
	assert.Equal(t, ".", relative)
}
