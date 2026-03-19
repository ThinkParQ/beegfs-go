package subscriber

import (
	"context"
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
	mu           sync.Mutex
	ackedSeqIDs  map[uint32]uint64 // keyed by MetaId
}

// Verify Service implements the SubscriberServer interface.
var _ bw.SubscriberServer = &Service{}

// NewService creates a subscriber handler that can be registered on any gRPC server. ackFrequency
// controls how often acknowledged sequence IDs are sent back to Watch (0 disables).
func NewService(log *zap.Logger, ackFrequency time.Duration, wg *sync.WaitGroup, grpcServer *grpc.Server) *Service {
	log = log.With(zap.String("component", path.Base(reflect.TypeFor[Service]().PkgPath())))
	s := &Service{
		log:          log,
		wg:           wg,
		ackFrequency: ackFrequency,
		ackedSeqIDs:  make(map[uint32]uint64),
	}
	bw.RegisterSubscriberServer(grpcServer, s)
	return s
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
			s.setAckedSeqID(ack.MetaId, ack.SeqId)
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
						if err := stream.Send(&bw.Response{CompletedSeq: seqID}); err != nil {
							s.log.Error("error sending ack to Watch", zap.Error(err), zap.Uint32("metaId", metaId), zap.Uint64("seqID", seqID))
						}
						lastAck = seqID
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

func (s *Service) setAckedSeqID(metaId uint32, seqId uint64) {
	s.mu.Lock()
	s.ackedSeqIDs[metaId] = seqId
	s.mu.Unlock()
}

func (s *Service) getAckedSeqID(metaId uint32) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ackedSeqIDs[metaId]
}
