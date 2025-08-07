package subscriber

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/types"
	pb "github.com/thinkparq/protobuf/go/beewatch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// A GRPCSubscriber implements a gRPC client that sends messages to a subscriber over gRPC.
// The subscriber must implement EventSubscriberServer defined by the BeeWatch API.
// As gRPC provides ordering but not deliver guarantees, when first connecting
// subscribers should acknowledge the sequence ID of the last received event.
// Otherwise duplicate events may be retransmitted to avoid dropped events.
type GRPCSubscriber struct {
	GrpcConfig
	conn       *grpc.ClientConn
	client     pb.SubscriberClient
	stream     pb.Subscriber_ReceiveEventsClient
	recvStream chan *pb.Response
	// recvStreamActive is used to ensure there is only ever one goroutine receiving responses.
	recvStreamActive bool
	// recvStreamActiveMu coordinates access to recvStreamActive.
	recvStreamActiveMu *sync.Mutex
}

var _ Interface = &GRPCSubscriber{} // Verify type satisfies interface.

func newGRPCSubscriber(config GrpcConfig) *GRPCSubscriber {
	var mutex sync.Mutex

	// Time we'll wait after closing our end of the connection for the subscriber to disconnect.
	if config.DisconnectTimeout == 0 {
		config.DisconnectTimeout = 30
	}

	return &GRPCSubscriber{
		GrpcConfig:         config,
		recvStreamActive:   false,
		recvStreamActiveMu: &mutex,
	}
}

// This is a "comparable" view of the GRPCSubscriber struct used for testing.
// When GRPCSubscriber is updated it should also be updated with any fields that are a comparable type.
type ComparableGRPCSubscriber struct {
	GrpcConfig
}

func (s *GRPCSubscriber) Connect() (retry bool, err error) {

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

	return false, nil
}

// Send attempts to transmit an event to a remote subscriber.
// It is expected to implement any logic for attempting to resend an event if the first attempt fails.
func (s *GRPCSubscriber) Send(event *pb.Event) (err error) {

	if err := s.stream.Send(event); err != nil {
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
	s.recvStreamActiveMu.Unlock()

	go func() {
		defer close(s.recvStream)
		defer func() {
			s.recvStreamActiveMu.Lock()
			s.recvStreamActive = false
			s.recvStreamActiveMu.Unlock()
		}()

		for {
			in, err := s.stream.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				// TODO: Now that we don't have a logger on the subscribers anymore, figure out how to handle.
				// One option is to have the recvStream channel actually send back a struct with a pb.Response and an error.
				// Then if the response is nil whatever is reading from recvStream can handle the error.
				// s.log.Error("failed to receive response", zap.Error(err))

				// TODO: Figure out if/how we want to handle this scenario better.
				// For example retry a few times or parse the error.
				// For now we'll treat it as a remote disconnect which will try to reconnect.
				// Ideally we don't log anything here (as is standard for the other methods).
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
