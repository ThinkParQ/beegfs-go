package subscriber

import (
	"path/filepath"
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

	// Feed an ack as a consumer would after fully processing event (7, 42). The drain loop advances
	// the in-memory cursor; wait until it has been applied.
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
