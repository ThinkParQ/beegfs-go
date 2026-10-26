package metadata

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/logger"
	"go.uber.org/zap"
)

// TestNewMarksV1Protocol verifies the shared event buffer records when the configured event
// protocol is v1, which has no handshake through which a metadata service can identify itself.
// Subscribers waiting on node ID detection rely on this to warn instead of waiting forever.
func TestNewMarksV1Protocol(t *testing.T) {
	newManager := func(t *testing.T, version string) *Manager {
		t.Helper()
		mgr, cleanup, err := New(context.Background(), &logger.Logger{Logger: zap.NewNop()}, []Config{{
			EventLogTarget:         filepath.Join(t.TempDir(), "events.sock"),
			EventBufferSize:        16,
			EventBufferGCFrequency: 4,
			EventVersion:           version,
		}})
		assert.NoError(t, err)
		t.Cleanup(cleanup)
		return mgr
	}

	assert.True(t, newManager(t, "1.0").EventBuffer.V1ProtocolInUse())
	assert.False(t, newManager(t, "2.0").EventBuffer.V1ProtocolInUse())
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
	// t.TempDir embeds the test name, which pushes the socket path past the 108-byte limit of a
	// Unix socket address. A short temp dir keeps the path under that limit.
	shortTempDir := func(t *testing.T) string {
		t.Helper()
		dir, err := os.MkdirTemp("", "watch")
		require.NoError(t, err)
		t.Cleanup(func() { os.RemoveAll(dir) })
		return dir
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
