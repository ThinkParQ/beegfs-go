package metadata

import (
	"context"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/logger"
	"go.uber.org/zap"
)

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "watch")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestNewCreatesSocketDirectories checks the modes of the directories New creates.
// Missing parents get 0755 and a missing socket directory gets 0700.
// Directories that already exist keep their modes.
// The test sets the umask to 022 because the umask strips bits from the modes New asks for.
func TestNewCreatesSocketDirectories(t *testing.T) {
	oldUmask := syscall.Umask(0o022)
	t.Cleanup(func() { syscall.Umask(oldUmask) })

	newManager := func(t *testing.T, socketPath string) {
		t.Helper()
		_, cleanup, err := New(context.Background(), &logger.Logger{Logger: zap.NewNop()}, []Config{{
			EventLogTarget:         socketPath,
			EventBufferSize:        16,
			EventBufferGCFrequency: 4,
		}})
		require.NoError(t, err)
		t.Cleanup(cleanup)
	}
	modeOf := func(t *testing.T, dir string) fs.FileMode {
		t.Helper()
		info, err := os.Stat(dir)
		require.NoError(t, err)
		return info.Mode().Perm()
	}

	t.Run("missing directories are created", func(t *testing.T) {
		tmp := shortTempDir(t)
		socketDir := filepath.Join(tmp, "run", "beegfs", "8a3e5f0c-uuid")
		newManager(t, filepath.Join(socketDir, "eventlog"))

		assert.Equal(t, fs.FileMode(0o755), modeOf(t, filepath.Join(tmp, "run")))
		assert.Equal(t, fs.FileMode(0o755), modeOf(t, filepath.Join(tmp, "run", "beegfs")))
		assert.Equal(t, fs.FileMode(0o700), modeOf(t, socketDir))
	})

	t.Run("existing parent directory keeps its mode", func(t *testing.T) {
		parentDir := filepath.Join(shortTempDir(t), "run", "beegfs")
		require.NoError(t, os.MkdirAll(parentDir, 0o755))
		require.NoError(t, os.Chmod(parentDir, 0o750))
		socketDir := filepath.Join(parentDir, "8a3e5f0c-uuid")
		newManager(t, filepath.Join(socketDir, "eventlog"))

		assert.Equal(t, fs.FileMode(0o750), modeOf(t, parentDir))
		assert.Equal(t, fs.FileMode(0o700), modeOf(t, socketDir))
	})

	t.Run("existing socket directory keeps its mode", func(t *testing.T) {
		socketDir := filepath.Join(shortTempDir(t), "run", "beegfs", "8a3e5f0c-uuid")
		require.NoError(t, os.MkdirAll(socketDir, 0o755))
		require.NoError(t, os.Chmod(socketDir, 0o750))
		newManager(t, filepath.Join(socketDir, "eventlog"))

		assert.Equal(t, fs.FileMode(0o750), modeOf(t, socketDir))
	})
}

// TestNewLocksSocketPath checks that New takes <socket>.lock before it touches the socket path.
// Every New call here runs in one process. That still tests the lock. A flock lock belongs to the
// open file, not to the process. So a second open of the lock file conflicts with the first.
func TestNewLocksSocketPath(t *testing.T) {
	newManager := func(socketPath string) (func(), error) {
		_, cleanup, err := New(context.Background(), &logger.Logger{Logger: zap.NewNop()}, []Config{{
			EventLogTarget:         socketPath,
			EventBufferSize:        16,
			EventBufferGCFrequency: 4,
		}})
		return cleanup, err
	}

	t.Run("second watch fails until the first cleans up", func(t *testing.T) {
		socketPath := filepath.Join(shortTempDir(t), "eventlog")
		lockPath := socketPath + ".lock"
		firstCleanup, err := newManager(socketPath)
		require.NoError(t, err)

		lockInfo, err := os.Stat(lockPath)
		require.NoError(t, err)
		assert.Equal(t, fs.FileMode(0o600), lockInfo.Mode().Perm(), "only the owner may open the lock file")

		secondCleanup, err := newManager(socketPath)
		if err == nil {
			secondCleanup()
		}
		require.EqualError(t, err, "another beegfs-watch is using the socket path "+socketPath+
			" (it holds the lock "+lockPath+"; run one beegfs-watch per socket path)")

		info, err := os.Stat(socketPath)
		require.NoError(t, err)
		assert.Equal(t, fs.ModeSocket, info.Mode().Type())

		firstCleanup()
		_, err = os.Stat(lockPath)
		require.NoError(t, err, "the lock file stays on disk after cleanup")

		thirdCleanup, err := newManager(socketPath)
		require.NoError(t, err)
		t.Cleanup(thirdCleanup)
	})

	t.Run("stale socket with no lock holder is removed", func(t *testing.T) {
		socketPath := filepath.Join(shortTempDir(t), "eventlog")
		// A watch killed with SIGKILL leaves its socket file behind. A listener that does not
		// unlink on close leaves the same file.
		stale, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socketPath, Net: "unixpacket"})
		require.NoError(t, err)
		stale.SetUnlinkOnClose(false)
		require.NoError(t, stale.Close())
		info, err := os.Stat(socketPath)
		require.NoError(t, err, "the stale socket file exists before New runs")
		require.Equal(t, fs.ModeSocket, info.Mode().Type())

		cleanup, err := newManager(socketPath)
		require.NoError(t, err)
		t.Cleanup(cleanup)

		conn, err := net.Dial("unixpacket", socketPath)
		require.NoError(t, err)
		conn.Close()
	})

	t.Run("directory at the socket path is rejected and kept", func(t *testing.T) {
		socketPath := filepath.Join(shortTempDir(t), "eventlog")
		require.NoError(t, os.Mkdir(socketPath, 0o700))

		cleanup, err := newManager(socketPath)
		if err == nil {
			cleanup()
		}
		require.ErrorContains(t, err, "existing directory")

		info, err := os.Stat(socketPath)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})
}
