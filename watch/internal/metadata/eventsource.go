package metadata

import (
	"sync/atomic"

	"github.com/thinkparq/beegfs-go/watch/internal/types"
)

type eventSource struct {
	metaID    uint32
	buffer    *types.MultiCursorRingBuffer
	lastSeqID atomic.Uint64
	attached  bool
}

func newEventSource(metaID uint32, bufferSize int, bufferGCFrequency int) *eventSource {
	buffer := types.NewMultiCursorRingBuffer(bufferSize, bufferGCFrequency)
	buffer.SetRingID(uint64(metaID))
	source := &eventSource{metaID: metaID, buffer: buffer}
	source.lastSeqID.Store(types.NoSeqId)
	return source
}

func nextSeqFromMeta(watchLastSeq uint64, metaOldestSeq uint64) uint64 {
	if watchLastSeq == types.NoSeqId {
		return metaOldestSeq
	}
	return max(watchLastSeq+1, metaOldestSeq)
}
