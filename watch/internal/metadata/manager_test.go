package metadata

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
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
