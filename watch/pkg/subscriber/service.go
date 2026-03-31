package subscriber

import (
	"context"
	"fmt"
	"io"
	"path"
	"reflect"
	"sync"
	"time"

	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ack carries a caller-confirmed sequence ID for a specific metadata server. After consuming an
// event, callers should send an Ack with the event's MetaId and SeqId to signal to Watch that the
// event has been fully processed.
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
// grpc.Server.Serve(). The expectation when stopping is the gRPC server calls grpc.Server.Stop()
// then waits at wg.Wait() for any inflight requests to complete.
type Service struct {
	bw.UnimplementedSubscriberServer
	log          *zap.Logger
	ackFrequency time.Duration
	wg           *sync.WaitGroup
	events       chan<- *bw.Event
	lastAcksMu   sync.RWMutex
	lastAcks     map[uint32]*lastAck // keyed by MetaId
	lastAckStore Checkpointer
}

type lastAck struct {
	mu    *sync.Mutex
	seqID uint64
}

// Verify Service implements the SubscriberServer interface.
var _ bw.SubscriberServer = &Service{}

// NewService creates a subscriber handler that can be registered on any gRPC server. ackFrequency
// controls how often acknowledged sequence IDs are sent back to Watch (0 disables).
func NewService(log *zap.Logger, ackFrequency time.Duration, wg *sync.WaitGroup, grpcServer *grpc.Server, checkpoint Checkpointer) (*Service, error) {
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
		wg:           wg,
		ackFrequency: ackFrequency,
		lastAcks:     lastAcks,
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
// carries the MetaId and SeqId of the processed event. Only acknowledged sequence IDs are forwarded
// to Watch, ensuring Watch does not consider an event delivered until the caller confirms it. Acks
// from different metadata servers are tracked independently. The caller is responsible for closing
// this channel when done.
func (s *Service) Start(events chan<- *bw.Event, acks <-chan Ack) {
	s.events = events

	// Drain acks from the caller and advance the acknowledged sequence ID per MetaId.
	go func() {
		for ack := range acks {
			if err := s.setAckedSeqID(ack.MetaId, ack.SeqId); err != nil {
				s.log.Error("unable to acknowledge sequence ID", zap.Any("MetaId", ack.MetaId), zap.Any("seqID", ack.SeqId), zap.Error(err))
			}
		}
	}()
}

// ReceiveEvents implements bw.SubscriberServer. It is called by the gRPC framework each time Watch
// establishes a new event stream connection.
//
// The MetaId is determined from the first received event and remains fixed for the lifetime of the
// connection. The per-MetaId acknowledged sequence ID is used for all acks sent back to this Watch
// instance.
func (s *Service) ReceiveEvents(stream bw.Subscriber_ReceiveEventsServer) error {
	s.wg.Add(1)
	defer s.wg.Done()

	ctx := stream.Context()

	// Receive the first event to determine the MetaId for this connection. This is safe because the
	// MetaId is stable for the lifetime of a Watch connection.
	event, err := s.recv(ctx, stream)
	if event == nil {
		return err // nil on clean EOF, non-nil on fatal error
	}

	metaId := event.GetMetaId()
	s.log.Info("Watch client connected", zap.Uint32("metaId", metaId))

	// Automatically init the lastAck for this meta the first time we see it. The lastAck.seqID
	// starts at 0, we will not attempt to send any acks below until an actual event is ack'd.
	s.lastAcksMu.Lock()
	if _, ok := s.lastAcks[metaId]; !ok {
		s.lastAcks[metaId] = &lastAck{
			mu:    &sync.Mutex{},
			seqID: 0,
		}
	}
	s.lastAcksMu.Unlock()

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
					if seqID != lastAck {
						s.log.Debug("sending event acknowledgement to Watch", zap.Any("metaID", metaId), zap.Any("seqID", seqID))
						if err := stream.Send(&bw.Response{CompletedSeq: seqID}); err != nil {
							s.log.Error("error sending acknowledgement to Watch", zap.Error(err), zap.Uint32("metaId", metaId), zap.Uint64("seqID", seqID))
						}
						lastAck = seqID
						if err := s.lastAckStore.Store(metaId, lastAck); err != nil {
							s.log.Error("unable to checkpoint sequence ID: duplicate events may be processed after a restart", zap.Error(err), zap.Uint32("metaId", metaId), zap.Uint64("seqID", seqID))
						}
					}
				}
			}
		}()
	}

	// Forward events to the caller. The loop starts with the first event already in hand,
	// then calls recv for each subsequent one.
	for event != nil {
		s.log.Debug("received event", zap.Any("event", event))

		select {
		case s.events <- event:
		case <-ctx.Done():
			s.log.Info("stream context cancelled while forwarding event", zap.Uint32("metaId", metaId))
			return ctx.Err()
		}

		event, err = s.recv(ctx, stream)
	}

	if err == nil {
		s.log.Info("Watch client closed the stream", zap.Uint32("metaId", metaId))
	}
	return err
}

// recv receives the next event from the stream, retrying on transient errors.
// Returns:
//   - (event, nil): a valid event was received
//   - (nil, nil): the stream closed cleanly (EOF)
//   - (nil, err): a fatal error occurred (context cancelled, etc.)
func (s *Service) recv(ctx context.Context, stream bw.Subscriber_ReceiveEventsServer) (*bw.Event, error) {
	for {
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
			if ok && st.Code() == codes.Canceled {
				return nil, ctx.Err()
			}
			s.log.Error("error receiving event from Watch", zap.Error(err))
			continue
		}

		return event, nil
	}
}

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
	ack.seqID = seqId
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
