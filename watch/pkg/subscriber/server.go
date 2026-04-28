package subscriber

import (
	"fmt"
	"net"
	"sync"
	"time"

	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Server wraps a Service with a dedicated gRPC server and listener. Use this when the subscriber
// needs its own address, separate from other services. For embedding subscriber endpoints on an
// existing gRPC server, use the Service directly.
type Server struct {
	*Service
	config     Config
	grpcServer *grpc.Server
	wg         *sync.WaitGroup
}

// Config holds configuration for a standalone Server with its own listener.
type Config struct {
	// Address is the local network address (host:port) the subscriber gRPC server will listen on.
	Address string `mapstructure:"address"`
	// TlsCertFile is the path to the certificate file used to identify this subscriber to Watch.
	TlsCertFile string `mapstructure:"tls-cert-file"`
	// TlsKeyFile is the path to the key file belonging to TlsCertFile.
	TlsKeyFile string `mapstructure:"tls-key-file"`
	// TlsDisable disables TLS entirely. Not recommended for production use.
	TlsDisable bool `mapstructure:"tls-disable"`
	// AckFrequency controls how often confirmed sequence IDs are sent back to Watch.
	// A value of 0 disables periodic acking entirely.
	AckFrequency time.Duration `mapstructure:"ack-frequency"`
}

// NewServer creates a Service and a dedicated gRPC server bound to config.Address.
func NewServer(log *zap.Logger, config Config, checkpoint Checkpointer, opts ...ServiceOption) (*Server, error) {
	wg := &sync.WaitGroup{}

	var grpcServerOpts []grpc.ServerOption
	if !config.TlsDisable && config.TlsCertFile != "" && config.TlsKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(config.TlsCertFile, config.TlsKeyFile)
		if err != nil {
			return nil, fmt.Errorf("subscriber: failed to load TLS credentials: %w", err)
		}
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(creds))
	} else {
		log.Warn("not using TLS because it was explicitly disabled or a certificate and/or key were not specified")
	}

	grpcServer := grpc.NewServer(grpcServerOpts...)
	svc, err := NewService(log, config.AckFrequency, wg, grpcServer, checkpoint, opts...)
	if err != nil {
		return nil, err
	}

	return &Server{
		Service:    svc,
		config:     config,
		grpcServer: grpcServer,
		wg:         wg,
	}, nil
}

// ListenAndServe calls Service.Start then starts the dedicated gRPC server in a goroutine.
// Fatal server errors are sent to errChan.
func (s *Server) ListenAndServe(events chan<- *bw.Event, acks <-chan Ack, errChan chan<- error) {
	s.Service.Start(events, acks)

	go func() {
		s.log.Info("listening on local network address", zap.String("address", s.config.Address))
		lis, err := net.Listen("tcp", s.config.Address)
		if err != nil {
			errChan <- fmt.Errorf("subscriber: error listening on address %s: %w", s.config.Address, err)
			return
		}
		s.log.Info("serving gRPC requests")
		if err := s.grpcServer.Serve(lis); err != nil {
			errChan <- fmt.Errorf("subscriber: error serving gRPC requests: %w", err)
		}
	}()
}

// Stop shuts down the dedicated gRPC server then waits for all in-progress ReceiveEvents
// handlers to finish. It uses grpcServer.Stop() (not GracefulStop) so that blocked
// stream.Recv() calls are cancelled immediately, allowing handlers to exit cleanly.
func (s *Server) Stop() {
	s.log.Info("stopping subscriber gRPC server")
	s.grpcServer.Stop()
	s.wg.Wait()
}
