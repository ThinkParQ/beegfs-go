package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	testTimeout           = 5 * time.Second
	testAckInterval       = 20 * time.Millisecond
	ackTicksWithoutEvents = 5
	storedSeqIDs          = "1,5,0\n2,2,0\n"
)

func startSubscriber(t *testing.T, dbFile string) (addr string, stop func()) {
	t.Helper()
	*mockDBFilename = dbFile
	*ackFrequency = testAckInterval
	log := zap.NewNop()

	dbCtx, dbShutdown := context.WithCancel(context.Background())
	db = newDB(dbCtx, log)
	var dbRunning sync.WaitGroup
	dbRunning.Add(1)
	go db.Run(&dbRunning)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	subscriberServer := NewEventSubscriberServer(log)
	bw.RegisterSubscriberServer(server, subscriberServer)
	go server.Serve(lis)

	stop = sync.OnceFunc(func() {
		server.Stop()
		subscriberServer.wg.Wait()
		dbShutdown()
		dbRunning.Wait()
	})
	t.Cleanup(stop)
	return lis.Addr().String(), stop
}

type watchStream struct {
	stream bw.Subscriber_ReceiveEventsClient
	metaID uint32
	acks   chan uint64
}

func dialAsWatch(t *testing.T, addr string, metaID uint32) *watchStream {
	t.Helper()
	conn, err := beegrpc.NewClientConn(addr,
		beegrpc.WithTLSDisable(true),
		beegrpc.WithNode(&beegfs.LegacyId{NodeType: beegfs.Meta, NumId: beegfs.NumId(metaID)}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	stream, err := bw.NewSubscriberClient(conn).ReceiveEvents(context.Background())
	require.NoError(t, err)

	s := &watchStream{stream: stream, metaID: metaID, acks: make(chan uint64, 1024)}
	go func() {
		for {
			response, err := stream.Recv()
			if err != nil {
				return
			}
			s.acks <- response.CompletedSeq
		}
	}()
	return s
}

func (s *watchStream) sendEvents(t *testing.T, seqIDs ...uint64) {
	t.Helper()
	for _, seqID := range seqIDs {
		require.NoError(t, s.stream.Send(&bw.Event{
			MetaId: s.metaID,
			SeqId:  seqID,
			EventData: &bw.Event_V2{V2: &bw.V2Event{
				Type: bw.V2Event_CREATE,
				Path: "/file",
			}},
		}))
	}
}

func (s *watchStream) nextAck(t *testing.T) uint64 {
	t.Helper()
	select {
	case ack := <-s.acks:
		return ack
	case <-time.After(testTimeout):
		require.FailNow(t, "no ack", "meta %d's stream got no ack", s.metaID)
		return 0
	}
}

func (s *watchStream) acksFromNow(t *testing.T, count int) []uint64 {
	t.Helper()
	for len(s.acks) > 0 {
		<-s.acks
	}
	acks := make([]uint64, count)
	for i := range acks {
		acks[i] = s.nextAck(t)
	}
	return acks
}

func (s *watchStream) waitForAck(t *testing.T, want uint64) {
	t.Helper()
	for {
		ack := s.nextAck(t)
		require.LessOrEqual(t, ack, want, "meta %d's stream was acked past its own events", s.metaID)
		if ack == want {
			return
		}
	}
}

func (s *watchStream) assertNoAckFor(t *testing.T, period time.Duration) {
	t.Helper()
	select {
	case ack := <-s.acks:
		assert.Failf(t, "unexpected ack", "meta %d's stream was acked with %d before it sent an event", s.metaID, ack)
	case <-time.After(period):
	}
}

func TestInitialResponseIsSentWithoutWaitingForAnAckTick(t *testing.T) {
	addr, _ := startSubscriber(t, filepath.Join(t.TempDir(), "seqids"))
	*ackFrequency = time.Hour

	meta1 := dialAsWatch(t, addr, 1)

	assert.Equal(t, uint64(0), meta1.nextAck(t))
}

func TestMetaThatSentNothingIsNotAckedUntilItsFirstEvent(t *testing.T) {
	addr, _ := startSubscriber(t, filepath.Join(t.TempDir(), "seqids"))
	meta1 := dialAsWatch(t, addr, 1)
	meta2 := dialAsWatch(t, addr, 2)
	require.Equal(t, uint64(0), meta2.nextAck(t), "meta 2's initial response asks watch to send from the start")

	meta1.sendEvents(t, 1, 2, 3, 4, 5)
	meta1.waitForAck(t, 5)
	meta2.assertNoAckFor(t, ackTicksWithoutEvents*testAckInterval)

	meta2.sendEvents(t, 0)
	meta2.waitForAck(t, 0)
}

func TestEventOnOneMetasStreamLeavesAnotherMetasAckUnchanged(t *testing.T) {
	addr, _ := startSubscriber(t, filepath.Join(t.TempDir(), "seqids"))
	meta1 := dialAsWatch(t, addr, 1)
	meta2 := dialAsWatch(t, addr, 2)
	meta1.sendEvents(t, 1, 2, 3, 4, 5)
	meta1.waitForAck(t, 5)

	meta2.sendEvents(t, 1)
	meta2.waitForAck(t, 1)

	assert.Equal(t, []uint64{5, 5, 5}, meta1.acksFromNow(t, 3))
}

func TestStopStoresOneSequenceIDLinePerMeta(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "seqids")
	addr, stop := startSubscriber(t, dbFile)
	meta1 := dialAsWatch(t, addr, 1)
	meta2 := dialAsWatch(t, addr, 2)
	meta1.sendEvents(t, 1, 2, 3, 4, 5)
	meta2.sendEvents(t, 1, 2)
	meta1.waitForAck(t, 5)
	meta2.waitForAck(t, 2)

	stop()

	data, err := os.ReadFile(dbFile)
	require.NoError(t, err)
	assert.Equal(t, storedSeqIDs, string(data))
}

func TestStartAcksEachMetaWithItsStoredSequenceID(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "seqids")
	require.NoError(t, os.WriteFile(dbFile, []byte(storedSeqIDs), 0644))

	addr, _ := startSubscriber(t, dbFile)
	meta1 := dialAsWatch(t, addr, 1)
	meta2 := dialAsWatch(t, addr, 2)

	assert.Equal(t, uint64(5), meta1.nextAck(t), "meta 1's first ack")
	assert.Equal(t, uint64(2), meta2.nextAck(t), "meta 2's first ack")
}
