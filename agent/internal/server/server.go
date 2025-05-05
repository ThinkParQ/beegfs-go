package server

import (
	"context"
	"fmt"
	"net"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/beegfs-go/agent/pkg/agent"
	"github.com/thinkparq/protobuf/go/beegfs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	Address     string `mapstructure:"address"`
	TlsCertFile string `mapstructure:"tls-cert-file"`
	TlsKeyFile  string `mapstructure:"tls-key-file"`
	TlsDisable  bool   `mapstructure:"tls-disable"`
}

type AgentServer struct {
	beegfs.UnimplementedAgentServer
	log *zap.Logger
	wg  *sync.WaitGroup
	Config
	grpcServer *grpc.Server
	reconciler agent.Reconciler
}

func New(log *zap.Logger, config Config, reconciler agent.Reconciler) (*AgentServer, error) {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(AgentServer{}).PkgPath())))

	s := AgentServer{
		log:        log,
		Config:     config,
		wg:         new(sync.WaitGroup),
		reconciler: reconciler,
	}
	var grpcServerOpts []grpc.ServerOption
	if !s.TlsDisable && s.TlsCertFile != "" && s.TlsKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(s.TlsCertFile, s.TlsKeyFile)
		if err != nil {
			return nil, err
		}
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(creds))
	} else {
		s.log.Warn("not using TLS because it was explicitly disabled or a certificate and/or key were not specified")
	}
	s.grpcServer = grpc.NewServer(grpcServerOpts...)
	beegfs.RegisterAgentServer(s.grpcServer, &s)
	return &s, nil
}

func (s *AgentServer) ListenAndServe(errChan chan<- error) {
	go func() {
		s.log.Info("listening on local network address", zap.Any("address", s.Address))
		lis, err := net.Listen("tcp", s.Address)
		if err != nil {
			errChan <- fmt.Errorf("remote server: error listening on the specified address %s: %w", s.Address, err)
			return
		}
		s.log.Info("serving gRPC requests")
		err = s.grpcServer.Serve(lis)
		if err != nil {
			errChan <- fmt.Errorf("remote server: error serving gRPC requests: %w", err)
		}
	}()
}

func (s *AgentServer) Stop() {
	s.log.Info("attempting to stop gRPC server")
	s.grpcServer.Stop()
	s.wg.Wait()
}

func (s *AgentServer) Apply(ctx context.Context, request *beegfs.AgentApplyRequest) (*beegfs.AgentResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	result, err := s.reconciler.Apply(ctx, request.Config)
	return &beegfs.AgentResponse{
		Status: result.Status,
	}, err
}

func (s *AgentServer) Destroy(ctx context.Context, request *beegfs.AgentDestroyRequest) (*beegfs.AgentResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	result, err := s.reconciler.Destroy(ctx, request.Config)
	return &beegfs.AgentResponse{
		Status: result.Status,
	}, err
}

func (s *AgentServer) Status(ctx context.Context, request *beegfs.AgentStatusRequest) (*beegfs.AgentResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	result, err := s.reconciler.Status(ctx)
	return &beegfs.AgentResponse{
		Status: result.Status,
	}, err
}
