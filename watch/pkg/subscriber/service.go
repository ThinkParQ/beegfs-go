package subscriber

import (
	"context"
	"fmt"
	"io"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Ack carries a caller-confirmed sequence ID for a specific metadata server. After consuming an
// event, callers send an Ack with the event's MetaId and SeqId to signal to Watch that the event
// has been fully processed.
//
// Every event received from the events channel MUST be acknowledged exactly once, including events
// the caller decides to skip or drop. Acks may be sent in any order (e.g. when events are consumed
// by parallel workers): the Service tracks a cumulative low watermark per meta and only reports an
// event as completed to Watch once it and every event forwarded before it have been acknowledged.
// An event that is never acknowledged therefore permanently halts that meta's watermark: no later
// event is ever reported as completed to Watch or checkpointed to disk (causing unbounded duplicate
// redelivery after a reconnect or restart), and the event's tracking entry is never freed.
// Acknowledging an event more than once, or acknowledging a sequence ID that was never delivered,
// is an error.
type Ack struct {
	MetaId uint32
	SeqId  uint64
}

// Service is the gRPC subscriber handler. It implements bw.SubscriberServer and can be registered
// on any *grpc.Server via Register.
//
// Watch acts as the gRPC client: it dials out to the subscriber and streams events to it. Multiple
// Watch instances (each proxying events for a different metadata server) may connect
// simultaneously. All events are delivered on a single channel; callers can distinguish their
// origin via bw.Event.MetaId. Acknowledgments are tracked per MetaId so each Watch connection
// receives accurate delivery confirmation independently.
//
// Use NewService to create and attach to an existing gRPC server, then call Start before
// grpc.Server.Serve(). To shut down, stop the gRPC server (so inflight ReceiveEvents handlers can
// exit) and close the acks channel passed to Start; closing acks tells the flusher to perform a
// final checkpoint flush and exit. The owner of the acks channel is responsible for closing it once
// all producers have stopped. Callers that need to confirm the final flush has reached disk can
// call WaitFlushed.
type Service struct {
	bw.UnimplementedSubscriberServer
	log          *zap.Logger
	ackFrequency time.Duration
	// wg tracks inflight ReceiveEvents handlers. It is owned by the Service and waited on by
	// Server.Stop once the gRPC server has been stopped.
	wg           sync.WaitGroup
	events       chan<- *bw.Event
	lastAcksMu   sync.RWMutex
	lastAcks     map[uint32]*lastAck // keyed by MetaId
	lastAckStore Checkpointer
	filter       *bw.EventFilter
	// flusherDone is closed by the flusher goroutine once it has drained the acks channel and
	// performed its final flush. WaitFlushed blocks on it.
	flusherDone chan struct{}
}

// ServiceOption is a functional option for NewService.
type ServiceOption func(*Service)

// WithEventFilter configures the event type filter sent to Watch in the initial Response.
// Watch will only deliver events whose types appear in the allowlists; an absent or empty
// list for a version means all events of that version are delivered.
func WithEventFilter(filter *bw.EventFilter) ServiceOption {
	return func(s *Service) { s.filter = filter }
}

type lastAck struct {
	mu *sync.Mutex
	// seqID is the cumulative acknowledged sequence ID (low watermark) for this meta: every event
	// forwarded to the consumer with a sequence ID at or below it has been fully processed. It is
	// what is sent to Watch as CompletedSeq and checkpointed to disk, both of which treat it
	// cumulatively.
	seqID uint64
	// pending holds events forwarded to the consumer, in stream order, that gate advancing seqID.
	// Entries are marked as they are acknowledged and removed once the contiguous prefix at the
	// front has completed. Its length is bounded by the number of events in flight (the events and
	// acks channel buffers plus the consumer's workers).
	pending []pendingEvent
}

// pendingEvent is a single forwarded-but-not-yet-settled event delivery.
type pendingEvent struct {
	seqID uint64
	acked bool
}

// Verify Service implements the SubscriberServer interface.
var _ bw.SubscriberServer = &Service{}

// NewService creates a subscriber handler that can be registered on any gRPC server. ackFrequency
// controls how often acknowledged sequence IDs are sent back to Watch (0 disables).
func NewService(log *zap.Logger, ackFrequency time.Duration, grpcServer *grpc.Server, checkpoint Checkpointer, opts ...ServiceOption) (*Service, error) {
	log = log.With(zap.String("component", path.Base(reflect.TypeFor[Service]().PkgPath())))

	storedLastAcks, err := checkpoint.Retrieve()
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve last checkpoint: %w", err)
	}

	lastAcks := make(map[uint32]*lastAck)
	for metaID, seqID := range storedLastAcks {
		lastAcks[metaID] = &lastAck{
			mu:    &sync.Mutex{},
			seqID: seqID,
		}
	}

	s := &Service{
		log:          log,
		ackFrequency: ackFrequency,
		lastAcks:     lastAcks,
		lastAckStore: checkpoint,
	}
	for _, opt := range opts {
		opt(s)
	}
	bw.RegisterSubscriberServer(grpcServer, s)
	return s, nil
}

// Start sets the events channel and begins draining the acks channel. Must be called before the
// gRPC server starts serving.
//
// events is a channel where incoming Watch events are delivered. The caller controls the buffer
// size; a buffered channel allows the subscriber to receive events faster than they are processed
// without blocking Watch.
//
// acks is a channel the caller writes an Ack to after an event has been fully consumed. Each Ack
// carries the MetaId and SeqId of the processed event. Every event delivered on events MUST be
// acknowledged exactly once — see Ack for the full contract and the consequences of a skipped ack.
// Only acknowledged sequence IDs are forwarded to Watch, ensuring Watch does not consider an event
// delivered until the caller confirms it. Acks from different metadata servers are tracked
// independently, and acks within one metadata server may arrive in any order: the sequence ID
// reported to Watch (and checkpointed to disk) only advances once an event and every event
// forwarded before it have been acknowledged. The caller owns this channel and must
// close it (once all producers have stopped) to shut the flusher down; that triggers a final
// checkpoint flush, after which WaitFlushed unblocks.
func (s *Service) Start(events chan<- *bw.Event, acks <-chan Ack) {
	s.events = events
	s.flusherDone = make(chan struct{})

	// Drain acks from the caller and advance the acknowledged sequence ID per MetaId while
	// periodically flushing the ackStore to disk.
	go func() {
		// flusherDone unblocks Stop once the final flush (on channel close) has completed.
		defer close(s.flusherDone)
		// If ackFrequency is 0 then no events are ack'd and the ackStore flusher never runs.
		var flushC <-chan time.Time
		if s.ackFrequency > 0 {
			ticker := time.NewTicker(s.ackFrequency)
			defer ticker.Stop()
			flushC = ticker.C
		}
		flush := func() {
			if err := s.lastAckStore.Flush(); err != nil {
				s.log.Error("unable to flush event checkpoints to disk, duplicate events may be processed after a restart", zap.Error(err))
			}
		}
		for {
			select {
			case ack, ok := <-acks:
				if !ok {
					flush()
					return
				}
				if err := s.setAckedSeqID(ack.MetaId, ack.SeqId); err != nil {
					s.log.Error("unable to acknowledge sequence ID", zap.Any("MetaId", ack.MetaId), zap.Any("seqID", ack.SeqId), zap.Error(err))
				}
			case <-flushC:
				flush()
			}
		}
	}()
}

// WaitFlushed blocks until the flusher has drained the acks channel and performed its final
// checkpoint flush to disk. The flusher exits after the acks channel passed to Start is closed, so
// WaitFlushed only returns once the owner has closed acks.
//
// It is optional: consumers that want to confirm the final checkpoint has reached disk before
// exiting can call it after closing acks. It must only be called after Start (otherwise the flusher
// goroutine does not exist and it blocks forever). It is safe to call from multiple goroutines and
// more than once.
func (s *Service) WaitFlushed() {
	<-s.flusherDone
}

// ReceiveEvents implements bw.SubscriberServer. It is called by the gRPC framework each time Watch
// establishes a new event stream connection.
//
// The MetaId is determined from the stream context remains fixed for the lifetime of the
// connection. The per-MetaId acknowledged sequence ID is used for all acks sent back to this Watch
// instance.
func (s *Service) ReceiveEvents(stream bw.Subscriber_ReceiveEventsServer) error {
	s.wg.Add(1)
	defer s.wg.Done()

	ctx := stream.Context()
	metadata, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		err := fmt.Errorf("event stream from watch does not include any metadata (ensure skip-node-id-detection is not set for this subscriber)")
		s.log.Error(err.Error())
		return err
	}
	nodeID := metadata.Get("node-id")
	if len(nodeID) == 0 {
		err := fmt.Errorf("event stream from watch does not include the metadata node-id (ensure skip-node-id-detection is not set for this subscriber)")
		s.log.Error(err.Error())
		return err
	}

	parser := beegfs.NewEntityIdParser(32, beegfs.Meta)
	entityID, err := parser.Parse(nodeID[0])
	if err != nil {
		err := fmt.Errorf("unable to parse a valid metadata node ID from the watch event stream: %w (ensure skip-node-id-detection is not set for this subscriber)", err)
		s.log.Error(err.Error())
		return err
	}
	legacyID, ok := entityID.(beegfs.LegacyId)
	if !ok {
		err := fmt.Errorf("successfully parsed metadata node entity ID from the watch event stream, but it is not a valid legacy ID: %s (ensure skip-node-id-detection is not set for this subscriber)", entityID)
		s.log.Error(err.Error())
		return err
	}
	// Downcast is safe because NewEntityIdParser already validated this is a 32-bit integer.
	metaId := uint32(legacyID.NumId)
	s.log.Info("watching for events from metadata node", zap.Any("metaID", metaId))

	// Automatically init the lastAck for this meta the first time we see it. types.SeekToEndSeqID tells
	// Watch to start streaming from the next event it receives from the metadata service, skipping
	// historical events that may no longer be relevant.
	s.lastAcksMu.Lock()
	if _, ok := s.lastAcks[metaId]; !ok {
		s.lastAcks[metaId] = &lastAck{
			mu:    &sync.Mutex{},
			seqID: types.SeekToEndSeqID,
		}
	}
	s.lastAcksMu.Unlock()

	// The default/recommended "wait-for-response-after-connect=0" requires Watch to wait for the
	// subscriber to ack the last event to minimize duplicate events. Thus if acks are enabled
	// always send an initial ack.
	initialAckSent := false

	// Periodically send the last caller-acknowledged sequence ID for this MetaId back to Watch.
	// This tells Watch which events have been fully consumed so it can advance its buffer.
	if s.ackFrequency > 0 {
		go func() {
			// lastAck is used to avoid sending duplicate acks when nothing has changed.
			var lastAck uint64
			ticker := time.NewTicker(s.ackFrequency)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					seqID := s.getAckedSeqID(metaId)
					if !initialAckSent || seqID != lastAck {
						s.log.Debug("sending event acknowledgement to watch", zap.Any("metaID", metaId), zap.Any("seqID", seqID))
						resp := &bw.Response{CompletedSeq: seqID}
						if !initialAckSent {
							// Include the filter only in the initial Response; Watch reads it once
							// during the connection handshake and ignores it in subsequent messages.
							resp.Filter = s.filter
						}
						if err := stream.Send(resp); err != nil {
							s.log.Error("error sending acknowledgement to watch", zap.Error(err), zap.Uint32("metaId", metaId), zap.Uint64("seqID", seqID))
							// A send error aborts the gRPC stream, so the stream is dead. The
							// stream context will be cancelled so the function will return at the
							// top of the next loop. When Watch reconnects the handshake will be
							// reattempted with a fresh initialAckSent. We still advance lastAck and
							// checkpoint below because that records local progress independent of
							// if Watch received this ack and minimizes duplicate replay on restart.
						}
						lastAck = seqID
						initialAckSent = true
						s.lastAckStore.Store(metaId, lastAck)
					}
				}
			}
		}()
	}

	// Forward events to the caller. The loop starts with the first event already in hand,
	// then calls recv for each subsequent one.
	event, err := s.recv(ctx, stream)
	for event != nil {
		s.log.Debug("received event", zap.Any("event", event))
		// Track the event before handing it to the consumer: once forwarded it can be acknowledged
		// at any moment, and the ack must always find its pending entry.
		if err := s.trackForwarded(event.MetaId, event.SeqId); err != nil {
			s.log.Error("unable to track event before forwarding it to the consumer", zap.Uint32("metaId", event.MetaId), zap.Uint64("seqID", event.SeqId), zap.Error(err))
			return err
		}
		select {
		case s.events <- event:
		case <-ctx.Done():
			// The event was never handed to the consumer, so no ack will ever arrive for it;
			// remove it so it cannot block this meta's watermark from advancing.
			s.untrackForwarded(event.MetaId, event.SeqId)
			s.log.Info("stream context cancelled while forwarding event from watch", zap.Uint32("metaId", metaId))
			return ctx.Err()
		}
		event, err = s.recv(ctx, stream)
	}

	if err != nil {
		s.log.Warn("watch closed the stream with an error", zap.Uint32("metaId", metaId), zap.Error(err))
	} else {
		s.log.Info("watch closed the stream gracefully", zap.Uint32("metaId", metaId))
	}
	return err
}

// recv receives the next event from the stream, retrying on transient errors.
// Returns:
//   - (event, nil): a valid event was received
//   - (nil, nil): the stream closed cleanly (EOF)
//   - (nil, err): a fatal error occurred (context cancelled, etc.)
func (s *Service) recv(ctx context.Context, stream bw.Subscriber_ReceiveEventsServer) (*bw.Event, error) {

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	event, err := stream.Recv()
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Canceled && st.Message() == "context canceled" {
			// Context cancellation signals a graceful shutdown was requested, no need to log.
			return nil, nil
		}
		s.log.Error("error receiving event from watch", zap.Error(err))
		return nil, err
	}

	return event, nil

}

// trackForwarded records an event that is about to be handed to the consumer so setAckedSeqID can
// tell when every event up to a given point has been processed. It MUST be called before the event
// is placed on the events channel: once forwarded, the event can be processed and acknowledged at
// any moment, and an ack for an untracked event is an error.
func (s *Service) trackForwarded(metaId uint32, seqId uint64) error {
	s.lastAcksMu.RLock()
	defer s.lastAcksMu.RUnlock()
	ack, ok := s.lastAcks[metaId]
	if !ok {
		// This should be impossible because the lastAck for a meta is initialized when its event
		// stream is established, before any of its events are forwarded. It could also indicate an
		// event whose MetaId does not match the node-id metadata of the stream that delivered it.
		return fmt.Errorf("unable to track an event for an unknown metadata node (metaId: %d, seqID: %d) (this is likely a bug)", metaId, seqId)
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	ack.pending = append(ack.pending, pendingEvent{seqID: seqId})
	return nil
}

// untrackForwarded removes the most recently tracked unacknowledged entry for seqId. It is used
// when an event was tracked but could not be handed to the consumer (the stream was cancelled
// mid-forward); leaving the entry in place would block the meta's watermark from ever advancing
// again since no ack can arrive for an event that was never forwarded.
func (s *Service) untrackForwarded(metaId uint32, seqId uint64) {
	s.lastAcksMu.RLock()
	defer s.lastAcksMu.RUnlock()
	ack, ok := s.lastAcks[metaId]
	if !ok {
		return
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	for i := len(ack.pending) - 1; i >= 0; i-- {
		if ack.pending[i].seqID == seqId && !ack.pending[i].acked {
			ack.pending = append(ack.pending[:i], ack.pending[i+1:]...)
			return
		}
	}
}

// setAckedSeqID marks the event (metaId, seqId) as processed and advances the meta's acknowledged
// sequence ID to the newest event where it and every event forwarded before it have been
// processed. Events are forwarded to the consumer in stream order but may be acknowledged out of
// order (e.g. the dispatch package processes events on parallel workers), while CompletedSeq and
// the on-disk checkpoint are cumulative: everything at or below them is treated as done. Advancing
// straight to the acknowledged seqId could therefore skip past an earlier event that is still
// being processed, and a crash after that value was checkpointed or sent to Watch would
// permanently lose the skipped event. Advancing a low watermark instead means a crash can only
// cause already-processed events to be replayed, preserving at-least-once delivery.
func (s *Service) setAckedSeqID(metaId uint32, seqId uint64) error {
	s.lastAcksMu.RLock()
	defer s.lastAcksMu.RUnlock()
	ack, ok := s.lastAcks[metaId]
	if !ok {
		// This should be impossible because the lastAck for a meta is initialized the first time we
		// start receiving events from it, but before those events are forwarded to the consumer.
		return fmt.Errorf("unable to acknowledge event for an unknown metadata node (this is likely a bug)")
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	// Mark the oldest unacknowledged delivery of this sequence ID. The same sequence ID can be
	// pending more than once when Watch redelivers events after a reconnect; each delivery is
	// consumed and acknowledged separately.
	found := false
	for i := range ack.pending {
		if ack.pending[i].seqID == seqId && !ack.pending[i].acked {
			ack.pending[i].acked = true
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("acknowledged an event that was not pending (metaId: %d, seqID: %d) (this is likely a bug)", metaId, seqId)
	}
	// Advance the watermark through the acknowledged prefix and drop those entries.
	completed := 0
	for _, p := range ack.pending {
		if !p.acked {
			break
		}
		// The watermark never moves backwards: duplicates redelivered after a reconnect can
		// complete behind it. The exception is types.SeekToEndSeqID, the initial sentinel for a meta
		// with no checkpoint, which is not real progress and is replaced by the first acknowledgment.
		if p.seqID > ack.seqID || ack.seqID == types.SeekToEndSeqID {
			ack.seqID = p.seqID
		}
		completed++
	}
	ack.pending = ack.pending[completed:]
	return nil
}

func (s *Service) getAckedSeqID(metaId uint32) uint64 {
	s.lastAcksMu.RLock()
	defer s.lastAcksMu.RUnlock()
	ack, ok := s.lastAcks[metaId]
	if !ok {
		return 0
	}
	ack.mu.Lock()
	defer ack.mu.Unlock()
	return ack.seqID
}
