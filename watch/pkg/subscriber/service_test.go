package subscriber

import (
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// newTestService builds a Service backed by a real DiskStore at path, attached to an unused gRPC
// server (NewService only registers the handler; the server is never served in these tests).
func newTestService(t *testing.T, path string, ackFrequency time.Duration) (*Service, *DiskStore) {
	t.Helper()
	ds, err := NewDiskStore(path)
	if err != nil {
		t.Fatalf("NewDiskStore: %v", err)
	}
	svc, err := NewService(zap.NewNop(), ackFrequency, grpc.NewServer(), ds)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, ds
}

// TestService_WaitFlushedBlocksUntilAcksClosed verifies WaitFlushed only returns once the acks
// channel is closed (the signal that drives the flusher's final flush and exit).
func TestService_WaitFlushedBlocksUntilAcksClosed(t *testing.T) {
	// A long ackFrequency means no periodic flush fires during the test, so closing acks is the only
	// thing that can unblock WaitFlushed.
	svc, _ := newTestService(t, filepath.Join(t.TempDir(), "acks.json"), time.Hour)
	events := make(chan *bw.Event, 1)
	acks := make(chan Ack)
	svc.Start(events, acks)

	done := make(chan struct{})
	go func() {
		svc.WaitFlushed()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("WaitFlushed returned before the acks channel was closed")
	case <-time.After(50 * time.Millisecond):
	}

	close(acks)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WaitFlushed did not return after the acks channel was closed")
	}
}

// TestService_WaitFlushedFlushesCheckpointOnClose verifies that closing acks drives a final
// checkpoint flush to disk and that WaitFlushed only returns once it has landed.
func TestService_WaitFlushedFlushesCheckpointOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acks.json")

	// Seed a checkpoint so meta 7 is known to the Service at startup (NewService loads it into the
	// in-memory cursor). Normally a meta is registered the first time ReceiveEvents sees it.
	seed, err := NewDiskStore(path)
	if err != nil {
		t.Fatal(err)
	}
	seed.Store(7, 10)
	if err := seed.Flush(); err != nil {
		t.Fatal(err)
	}

	// A long ackFrequency disables periodic flushing, so the checkpoint can only be written by the
	// final flush triggered when acks is closed.
	svc, ds := newTestService(t, path, time.Hour)
	events := make(chan *bw.Event, 1)
	acks := make(chan Ack)
	svc.Start(events, acks)

	// Track the event as ReceiveEvents would before forwarding it, then feed an ack as a consumer
	// would after fully processing event (7, 42). The drain loop advances the in-memory cursor;
	// wait until it has been applied.
	if err := svc.trackForwarded(7, 42); err != nil {
		t.Fatal(err)
	}
	acks <- Ack{MetaId: 7, SeqId: 42}
	deadline := time.Now().Add(time.Second)
	for svc.getAckedSeqID(7) != 42 {
		if time.Now().After(deadline) {
			t.Fatal("drain loop did not apply the ack")
		}
		time.Sleep(time.Millisecond)
	}

	// The ReceiveEvents handler periodically copies the in-memory cursor into the checkpoint store
	// (and forwards it to Watch). This test does not open a gRPC stream, so perform that one step
	// directly to mirror what the handler would do.
	ds.Store(7, svc.getAckedSeqID(7))

	// The flush has not run yet, so on disk the checkpoint still holds the seeded value.
	if before, err := NewDiskStore(path); err != nil {
		t.Fatal(err)
	} else if got, _ := before.Retrieve(); got[7] != 10 {
		t.Fatalf("expected seeded seqID 10 on disk before close, got %d", got[7])
	}

	// Closing acks triggers the final flush; WaitFlushed returns once it has reached disk.
	close(acks)
	svc.WaitFlushed()

	reopened, err := NewDiskStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Retrieve()
	if err != nil {
		t.Fatal(err)
	}
	if got[7] != 42 {
		t.Fatalf("expected checkpoint to reflect seqID 42 for metaID 7, got %d", got[7])
	}
}

// TestService_AckWatermark verifies the acknowledged sequence ID reported per meta is a cumulative
// low watermark: it only advances once an event and every event forwarded before it have been
// acknowledged, regardless of the order acks arrive in. Advancing past a still-in-flight event
// would let a crash permanently skip it, because both CompletedSeq and the checkpoint are
// cumulative.
func TestService_AckWatermark(t *testing.T) {
	svc, _ := newTestService(t, filepath.Join(t.TempDir(), "acks.json"), time.Hour)

	// Register meta 1 the way ReceiveEvents does for a meta with no checkpoint: the initial
	// math.MaxUint64 sentinel asks Watch to stream from the end of its buffer.
	svc.lastAcksMu.Lock()
	svc.lastAcks[1] = &lastAck{mu: &sync.Mutex{}, seqID: math.MaxUint64}
	svc.lastAcksMu.Unlock()

	track := func(seqID uint64) {
		t.Helper()
		if err := svc.trackForwarded(1, seqID); err != nil {
			t.Fatal(err)
		}
	}
	ackEvent := func(seqID uint64) {
		t.Helper()
		if err := svc.setAckedSeqID(1, seqID); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(want uint64) {
		t.Helper()
		if got := svc.getAckedSeqID(1); got != want {
			t.Fatalf("expected acked seqID %d, got %d", want, got)
		}
	}

	t.Run("out-of-order acks advance only through the completed prefix", func(t *testing.T) {
		track(10)
		track(11)
		track(12)
		// Event 11 completes while 10 is still being processed: the watermark must not move, or a
		// crash now would skip event 10.
		ackEvent(11)
		expect(math.MaxUint64)
		// Once 10 completes the contiguous prefix (10, 11) is done.
		ackEvent(10)
		expect(11)
		ackEvent(12)
		expect(12)
	})

	t.Run("gaps from filtered events do not block the watermark", func(t *testing.T) {
		// Watch filters events before sending, so consecutive forwarded events can have
		// non-consecutive sequence IDs. Completion is judged by forwarding order, not seqID+1.
		track(20)
		track(25)
		track(30)
		ackEvent(25)
		expect(12)
		ackEvent(20)
		expect(25)
		ackEvent(30)
		expect(30)
	})

	t.Run("duplicates redelivered after a reconnect never regress the watermark", func(t *testing.T) {
		// If Watch never received our acks before a reconnect it redelivers events 25 and 30 even
		// though the watermark already covers them. Each delivery is acknowledged separately and
		// must not move the watermark backwards.
		track(25)
		track(30)
		ackEvent(25)
		expect(30)
		ackEvent(30)
		expect(30)
	})

	t.Run("acking an event that was never forwarded is an error", func(t *testing.T) {
		if err := svc.setAckedSeqID(1, 999); err == nil {
			t.Fatal("expected an error acknowledging an event that was never forwarded")
		}
		expect(30)
	})

	t.Run("untracked events do not block the watermark", func(t *testing.T) {
		// Simulates ReceiveEvents tracking an event and then failing to forward it because the
		// stream was cancelled: the entry is removed so later events can still advance the
		// watermark.
		track(40)
		track(41)
		svc.untrackForwarded(1, 41)
		ackEvent(40)
		expect(40)
		track(42)
		ackEvent(42)
		expect(42)
	})
}
