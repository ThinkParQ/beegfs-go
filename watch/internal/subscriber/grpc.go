package subscriber

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/types"
	pb "github.com/thinkparq/protobuf/go/beewatch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// streamProbeTimeout bounds how long Connect waits to learn whether the subscriber accepted the
	// event stream (see probeStream). A rejection arrives within one round trip, so this only needs
	// to cover a slow network or a busy subscriber. A subscriber that sends nothing until its first
	// response delays every connect by this long.
	streamProbeTimeout = 2 * time.Second
)

// A GRPCSubscriber implements a gRPC client that sends messages to a subscriber over gRPC.
// The subscriber must implement EventSubscriberServer defined by the BeeWatch API.
// As gRPC provides ordering but not deliver guarantees, when first connecting
// subscribers should acknowledge the sequence ID of the last received event.
// Otherwise duplicate events may be retransmitted to avoid dropped events.
type GRPCSubscriber struct {
	GrpcConfig
	// authSecret is sent to the subscriber with every call. It is nil unless the subscriber sets
	// grpc-send-auth-secret.
	authSecret []byte
	conn       *grpc.ClientConn
	client     pb.SubscriberClient
	stream     pb.Subscriber_ReceiveEventsClient
	// headerDone closes when the Header() call that probeStream started on stream returns. Receive
	// waits for it before its first Recv, so the two never read from the stream at once. It is nil
	// until Connect opens a stream.
	headerDone chan struct{}
	recvStream chan *pb.Response
	// recvStreamActive is used to ensure there is only ever one goroutine receiving responses.
	recvStreamActive bool
	// recvErr is the error that ended the most recent receive goroutine, or nil if it ended cleanly
	// or is still running.
	recvErr error
	// recvStreamActiveMu coordinates access to recvStreamActive and recvErr.
	recvStreamActiveMu *sync.Mutex
}

var _ Interface = &GRPCSubscriber{} // Verify type satisfies interface.

// newGRPCSubscriber keeps authSecret only when config opts in with SendAuthSecret.
func newGRPCSubscriber(config GrpcConfig, authSecret []byte) *GRPCSubscriber {
	var mutex sync.Mutex

	// Time we'll wait after closing our end of the connection for the subscriber to disconnect.
	if config.DisconnectTimeout == 0 {
		config.DisconnectTimeout = 30
	}

	if !config.SendAuthSecret {
		authSecret = nil
	}

	return &GRPCSubscriber{
		GrpcConfig:         config,
		authSecret:         authSecret,
		recvStreamActive:   false,
		recvStreamActiveMu: &mutex,
	}
}

// This is a "comparable" view of the GRPCSubscriber struct used for testing.
// When GRPCSubscriber is updated it should also be updated with any fields that are a comparable type.
type ComparableGRPCSubscriber struct {
	GrpcConfig
}

func (s *GRPCSubscriber) Connect(forMetaID beegfs.NumId) (retry bool, err error) {

	var cert []byte
	if !s.TlsDisable && s.TLSCertFile != "" {
		cert, err = os.ReadFile(s.TLSCertFile)
		if err != nil {
			return false, fmt.Errorf("reading certificate file failed: %w", err)
		}
	}
	s.conn, err = beegrpc.NewClientConn(
		s.Address,
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithTLSDisableVerification(s.TLSDisableVerification),
		beegrpc.WithTLSDisable(s.TlsDisable),
		beegrpc.WithProxy(s.UseProxy),
		beegrpc.WithAuthSecret(s.authSecret),
		beegrpc.WithNode(&beegfs.LegacyId{
			NodeType: beegfs.Meta,
			// This will be zero if node ID detection is skipped. That is an invalid NumId which
			// will cause the caller to be unable to parse an entity ID from the legacy ID.
			NumId: forMetaID,
		}),
		// ForceCodecV2 is experimental but has no wire-compatible alternative: rawBytesCodec.Name()
		// returns "proto", keeping the content-type as application/grpc+proto, and Marshal passes
		// pre-marshaled bytes through unchanged, so the framing is byte-for-byte identical to the
		// default proto codec. Any future replacement for ForceCodecV2 must preserve both invariants
		// or it will silently break compatibility with subscribers.
		beegrpc.WithDialOptions(grpc.WithDefaultCallOptions(grpc.ForceCodecV2(rawBytesCodec{}))),
	)
	if err != nil {
		return true, fmt.Errorf("unable to connect to the subscriber: %w", err)
	}

	s.client = pb.NewSubscriberClient(s.conn)

	// We don't use a real context here because disconnect() handles cleaning up the stream.
	// https://github.com/grpc/grpc-go/blob/v1.56.0/stream.go#L141
	s.stream, err = s.client.ReceiveEvents(context.TODO())

	if err != nil {
		if st, ok := status.FromError(err); ok {
			// TLS misconfiguration can cause a confusing error message so we handle it explicitly.
			// Note this is just a hint to the user, other error conditions may have the same
			// message so we don't adjust behavior (i.e., treat it as fatal).
			if strings.Contains(st.Message(), "error reading server preface: EOF") {
				return true, fmt.Errorf("%w (hint: check TLS is configured correctly on the client and server)", err)
			}
		}
		return true, fmt.Errorf("unable to setup gRPC client stream: %w", err)
	}

	if err := s.probeStream(); err != nil {
		return true, err
	}
	return false, nil
}

// probeStream runs in Connect, right after the event stream opens. It decides whether the
// subscriber rejected the stream, so Connect can fail and the handler retries with backoff.
// Without it, a rejected stream looks connected, and the handler only sees a bare io.EOF when it
// next sends an event.
//
// Two facts about grpc-go make the probe necessary and make it work:
//   - Opening a bidi stream does not wait for the server. A server that rejects the stream, for
//     example in an authentication interceptor, ends it with a status and no headers.
//   - A server that accepts the stream sends headers with its first message, or earlier if it
//     calls SendHeader. Header() blocks until headers arrive or the stream ends. It returns nil
//     when the stream ended without headers, and the status is then read with Recv.
//
// The subscriber library in watch/pkg/subscriber calls SendHeader as soon as it accepts a stream.
// probeStream applies these rules:
//   - Headers arrive: the subscriber accepted the stream.
//   - The stream ends without headers: return its status as an error.
//   - Nothing arrives within streamProbeTimeout: treat the stream as accepted. A subscriber that
//     stays silent until its first response lands here, so it behaves as it did before the probe.
//
// After a timeout, Header() keeps running until headers arrive or the stream ends. It closes
// s.headerDone when it returns.
func (s *GRPCSubscriber) probeStream() error {
	headerDone := make(chan struct{})
	s.headerDone = headerDone
	// accepted is written before headerDone closes and read only after, so it needs no lock.
	accepted := false
	go func() {
		defer close(headerDone)
		md, _ := s.stream.Header()
		accepted = md != nil
	}()

	select {
	case <-headerDone:
		if accepted {
			return nil
		}
		_, err := s.stream.Recv()
		if err == io.EOF {
			return errors.New("subscriber ended the event stream as soon as it was opened")
		}
		return fmt.Errorf("subscriber rejected the event stream: %w", streamError(err))
	case <-time.After(streamProbeTimeout):
		return nil
	}
}

// streamError adds an operator hint to the status that ended an event stream. It runs on the status
// that probeStream reads and on the error that ends the receive goroutine. It returns err unchanged
// for codes that need no hint.
func streamError(err error) error {
	if status.Code(err) == codes.Unauthenticated {
		return fmt.Errorf("%w (hint: the subscriber requires BeeGFS connection authentication: set grpc-send-auth-secret = true for this subscriber, and make sure management.auth-file holds the same secret as the subscriber's BeeGFS client)", err)
	}
	return err
}

// Send attempts to transmit a pre-marshaled event to a remote subscriber. It is expected to
// implement any logic for attempting to resend an event if the first attempt fails.
func (s *GRPCSubscriber) Send(event []byte) (err error) {
	if err := s.stream.SendMsg(event); err != nil {
		// TODO: Is there ever a scenario where we'd want to retry to send the event?
		return fmt.Errorf("unable to send event to subscriber: %w", err)
	}
	return nil
}

// Receive starts a Go routine that receives events from the subscriber.
// It returns a channel where responses from the subscriber will be sent.
// Normally this channel is read by methods of the handler.
// Receive() is idempotent, and multiple calls will return the same recvStream channel.
func (s *GRPCSubscriber) Receive() (recvStream chan *pb.Response) {

	// This is how we guarantee the method is idempotent and there is only one goroutine listening to responses.
	// It also guarantees we don't try and reinitialize an in-use channel until it is closed.
	s.recvStreamActiveMu.Lock()
	if s.recvStreamActive {
		s.recvStreamActiveMu.Unlock()
		// TODO: We don't have a logger on the subscribers anymore.
		// Do we care enough about this to return it as a response somehow? Ideally as a specific error type?
		// s.log.Warn("already listening for responses from this subscriber (returning existing receive stream channel)")
		return s.recvStream
	}

	// If the channel was not yet initialized or closed we'll reinitialize it:
	s.recvStream = make(chan *pb.Response)
	s.recvStreamActive = true
	s.recvErr = nil
	s.recvStreamActiveMu.Unlock()

	headerDone := s.headerDone
	go func() {
		defer close(s.recvStream)
		defer func() {
			s.recvStreamActiveMu.Lock()
			s.recvStreamActive = false
			s.recvStreamActiveMu.Unlock()
		}()

		// Wait for the Header() call probeStream started. Recv could not return a response before
		// the headers anyway, so this delays nothing.
		if headerDone != nil {
			<-headerDone
		}

		for {
			in, err := s.stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				// Keep the error for the handler, which logs it once the channel closes (see
				// ReceiveErr). The handler then treats it as a remote disconnect and reconnects.
				s.recvStreamActiveMu.Lock()
				s.recvErr = streamError(err)
				s.recvStreamActiveMu.Unlock()
				return
			}
			s.recvStream <- in
			// TODO: Do we ever need to worry about leaking goroutines here?
			// I don't think so because if the app is shutting down or the subscriber disconnected,
			// we don't try to send on the channel and just return.
			// It'd be a corner case where we got a response, but the subscriber handler had already
			// stopped listening to the receive stream.
		}
	}()

	return s.recvStream
}

// ReceiveErr returns the error that ended the most recent receive goroutine. It is nil when the
// subscriber closed the stream cleanly, or while the goroutine still runs. The handler calls it
// after the channel from Receive closes, and the close orders it after the goroutine set the error.
func (s *GRPCSubscriber) ReceiveErr() error {
	s.recvStreamActiveMu.Lock()
	defer s.recvStreamActiveMu.Unlock()
	return s.recvErr
}

// Disconnect handles cleaning up all resources used for the gRPC connection.
// It can be called at any point in the lifecycle of a connection.
// For example even if a connection was only partially established, it can be used to cleanup.
// It will also attempt to receive any additional responses from the subscriber.
// For example final acknowledgement of events that were already sent.
//
// It works as follows:
// * First we attempt to close our end of the stream.
// * If the subscriber is not already disconnected this should prompt them to wrap up and disconnect.
// * If they don't disconnect within a configurable timeout we'll try to close the connection anyway.
//   - We'll return an error and let the caller decide if they want to try and disconnect again.
func (s *GRPCSubscriber) Disconnect() error {

	var multiErr types.MultiError

	// The stream could be nil if we were never connected.
	// We'll get a segmentation violation if we don't check.
	if s.stream != nil {
		if err := s.stream.CloseSend(); err != nil {
			err = fmt.Errorf("an error occurred closing the send direction of the subscriber stream: %w", err)
			multiErr.Errors = append(multiErr.Errors, err)
		} // TODO: Handle the error if the stream was already closed.
	}

	//
	if s.recvStream != nil {
	responseLoop:
		for {
			select {
			case _, ok := <-s.recvStream:
				if !ok {
					break responseLoop // Subscriber has already disconnected.
				}
				// TODO: https://linear.app/thinkparq/issue/BF-29/acknowledge-events-sent-to-all-subscribers-back-to-the-metadata-server
				// Since subscribers don't have a logger anymore, if we need to log this for debugging figure another way.
				// s.log.Info("received response from subscriber", zap.Any("response", response))
				continue responseLoop
			case <-time.After(time.Duration(s.DisconnectTimeout) * time.Second):
				// This indicates the subscriber didn't close the stream we're receiving responses from them in time.
				// Maybe they're hung or the connection is broken.
				err := fmt.Errorf("subscriber failed to disconnect within the %ds timeout", s.DisconnectTimeout)
				multiErr.Errors = append(multiErr.Errors, err)
				break responseLoop
			}
		}
	}

	// The connection could be nil if we were never connected.
	// We'll get a segmentation violation if we don't check.
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			err = fmt.Errorf("an error ocurred closing the subscriber connection: %w", err)
			multiErr.Errors = append(multiErr.Errors, err)
		} // TODO: Handle the error if the connection was already closed.
	}

	if len(multiErr.Errors) > 0 {
		return &multiErr
	}

	return nil
}
