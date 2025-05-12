package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
	"github.com/thinkparq/beegfs-go/agent/pkg/reconciler"
	"github.com/thinkparq/protobuf/go/beegfs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type Config struct {
	Address     string `mapstructure:"address"`
	TlsCertFile string `mapstructure:"tls-cert-file"`
	TlsKeyFile  string `mapstructure:"tls-key-file"`
	TlsDisable  bool   `mapstructure:"tls-disable"`
}

type AgentServer struct {
	beegfs.UnimplementedBeeAgentServer
	log *zap.Logger
	wg  *sync.WaitGroup
	Config
	grpcServer *grpc.Server
	reconciler reconciler.Reconciler
}

func New(log *zap.Logger, config Config, reconciler reconciler.Reconciler) (*AgentServer, error) {
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
	beegfs.RegisterBeeAgentServer(s.grpcServer, &s)
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

func (s *AgentServer) Update(ctx context.Context, request *beegfs.AgentUpdateRequest) (*beegfs.AgentUpdateResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	filesystems := make(map[string]manifest.Filesystem, len(request.GetConfig()))
	for fsUUID, protoFS := range request.GetConfig() {
		if protoFS == nil {
			return nil, status.Error(codes.InvalidArgument, "file system configuration was unexpectedly nil for fsUUID "+fsUUID)
		}
		filesystems[fsUUID] = manifest.FromProto(protoFS)
	}

	if err := s.reconciler.UpdateConfiguration(filesystems); err != nil {
		return nil, grpcStatusFrom(err)
	}
	return &beegfs.AgentUpdateResponse{
		AgentId: s.reconciler.GetAgentID(),
	}, nil
}

func (s *AgentServer) Status(ctx context.Context, request *beegfs.AgentStatusRequest) (*beegfs.AgentStatusResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	if result, err := s.reconciler.Status(); err != nil {
		return nil, grpcStatusFrom(err)
	} else {
		return &beegfs.AgentStatusResponse{
			Status:  result.Status,
			AgentId: s.reconciler.GetAgentID(),
		}, nil
	}
}

func (s *AgentServer) Cancel(ctx context.Context, request *beegfs.AgentCancelRequest) (*beegfs.AgentCancelResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	if result, err := s.reconciler.Cancel(request.GetReason()); err != nil {
		return nil, grpcStatusFrom(err)
	} else {
		return &beegfs.AgentCancelResponse{
			Status:  result.Status,
			AgentId: s.reconciler.GetAgentID(),
		}, nil
	}
}

func grpcStatusFrom(err error) error {
	var grpcErr error
	switch {
	case errors.Is(err, reconciler.ErrSavingManifest):
		grpcErr = status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, reconciler.ErrBadManifest):
		grpcErr = status.Error(codes.InvalidArgument, err.Error())
	default:
		grpcErr = status.Error(codes.Unknown, err.Error())
	}
	return grpcErr
}
