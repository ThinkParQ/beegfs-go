package server

import (
	"context"
	"fmt"
	"net"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/registry"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/workmgr"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	Address     string `mapstructure:"address"`
	TlsCertFile string `mapstructure:"tls-cert-file"`
	TlsKeyFile  string `mapstructure:"tls-key-file"`
	TlsDisable  bool   `mapstructure:"tls-disable"`
}

var _ flex.WorkerNodeServer = &WorkerNodeServer{}

type WorkerNodeServer struct {
	flex.UnimplementedWorkerNodeServer
	log *zap.Logger
	wg  *sync.WaitGroup
	Config
	grpcServer *grpc.Server
	workMgr    *workmgr.Manager
	registry   *registry.ComponentRegistry
	startTime  time.Time
}

// New() creates a new WorkerNodeServer that can be used with ListenAndServe().
func New(log *zap.Logger, config Config, workMgr *workmgr.Manager, buildInfo *flex.BuildInfo, features map[string]*flex.Feature) (*WorkerNodeServer, error) {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(WorkerNodeServer{}).PkgPath())))

	s := WorkerNodeServer{
		log:       log,
		wg:        new(sync.WaitGroup),
		Config:    config,
		workMgr:   workMgr,
		registry:  registry.NewComponentRegistry(buildInfo, features),
		startTime: time.Now(),
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
	flex.RegisterWorkerNodeServer(s.grpcServer, &s)

	return &s, nil
}

// ListenAndServe should be called against a WorkerNodeServer initialized with New(). It spawns a
// new goroutine to handle serving requests until an an error occurs or Stop() is called against the
// WorkerNodeServer. It accepts an errChan where any errors will be returned if the gRPC server
// terminates early unexpectedly.
func (s *WorkerNodeServer) ListenAndServe(errChan chan<- error) {
	go func() {
		s.log.Info("listening on local network address", zap.Any("address", s.Address))
		lis, err := net.Listen("tcp", s.Address)
		if err != nil {
			errChan <- fmt.Errorf("worker node server: error listening on the specified address %s: %w", s.Address, err)
			return
		}
		s.log.Info("serving gRPC requests")
		err = s.grpcServer.Serve(lis)
		if err != nil {
			errChan <- fmt.Errorf("worker node server: error serving gRPC requests: %w", err)
		}
	}()
}

// Stop should be called to gracefully terminate the server. It will stop the
// server then wait for outstanding RPCs to complete before returning.
func (s *WorkerNodeServer) Stop() {
	s.log.Info("attempting to stop gRPC server")
	s.grpcServer.Stop()
	s.wg.Wait()
}

func (s *WorkerNodeServer) UpdateConfig(ctx context.Context, request *flex.UpdateConfigRequest) (*flex.UpdateConfigResponse, error) {
	s.log.Info("attempting to apply new configuration")
	err := s.workMgr.UpdateConfig(request.GetRsts(), request.GetBeeRemote())
	if err != nil {
		s.log.Error("error applying new configuration", zap.Error(err))
		return flex.UpdateConfigResponse_builder{
			Result:  flex.UpdateConfigResponse_FAILURE,
			Message: err.Error(),
		}.Build(), nil
	}
	s.log.Info("successfully applied new configuration")
	return flex.UpdateConfigResponse_builder{
		Result:  flex.UpdateConfigResponse_SUCCESS,
		Message: "successfully applied updated configuration",
	}.Build(), nil

}

func (s *WorkerNodeServer) BulkUpdateWork(ctx context.Context, request *flex.BulkUpdateWorkRequest) (*flex.BulkUpdateWorkResponse, error) {
	s.log.Debug("attempting to update existing work requests", zap.Any("request", request))
	// TODO: https://github.com/ThinkParQ/bee-remote/issues/56
	// Allow bulk updates to work requests.
	if request.GetNewState() != flex.BulkUpdateWorkRequest_UNCHANGED {
		return flex.BulkUpdateWorkResponse_builder{
			Success: false,
			Message: "unable to update work requests, new state is unknown: %s" + request.GetNewState().String(),
		}.Build(), nil
	}
	return flex.BulkUpdateWorkResponse_builder{
		Success: true,
		Message: "",
	}.Build(), nil
}

func (s *WorkerNodeServer) SubmitWork(ctx context.Context, request *flex.SubmitWorkRequest) (*flex.SubmitWorkResponse, error) {
	s.log.Debug("received work request", zap.Any("request", request))
	work, err := s.workMgr.SubmitWorkRequest(request.GetRequest())
	if err != nil {
		return nil, err
	}
	return flex.SubmitWorkResponse_builder{Work: work}.Build(), nil
}

func (s *WorkerNodeServer) UpdateWork(ctx context.Context, request *flex.UpdateWorkRequest) (*flex.UpdateWorkResponse, error) {
	s.log.Debug("attempting to update existing work request", zap.Any("request", request))
	work, err := s.workMgr.UpdateWork(request)
	if err != nil {
		return nil, err
	}
	return flex.UpdateWorkResponse_builder{Work: work}.Build(), nil
}

func (s *WorkerNodeServer) Heartbeat(ctx context.Context, request *flex.HeartbeatRequest) (*flex.HeartbeatResponse, error) {
	s.log.Debug("processing heartbeat request", zap.Any("request", request))
	//ready := s.workMgr.IsReady()
	return flex.HeartbeatResponse_builder{
		IsReady: s.workMgr.IsReady(),
	}.Build(), nil
}

func (s *WorkerNodeServer) GetCapabilities(ctx context.Context, request *flex.GetCapabilitiesRequest) (*flex.GetCapabilitiesResponse, error) {
	return &flex.GetCapabilitiesResponse{
		BuildInfo:      s.registry.GetBuildInfo(),
		Features:       s.registry.GetCapabilities(),
		StartTimestamp: timestamppb.New(s.startTime),
	}, nil
}
