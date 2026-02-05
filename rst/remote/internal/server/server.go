package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/kvstore"
	"github.com/thinkparq/beegfs-go/common/registry"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/job"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	Address     string `mapstructure:"address"`
	TlsCertFile string `mapstructure:"tls-cert-file"`
	TlsKeyFile  string `mapstructure:"tls-key-file"`
	TlsDisable  bool   `mapstructure:"tls-disable"`
}

// Verify interfaces are satisfied:
var _ beeremote.BeeRemoteServer = &BeeRemoteServer{}

type BeeRemoteServer struct {
	beeremote.UnimplementedBeeRemoteServer
	log *zap.Logger
	wg  *sync.WaitGroup
	Config
	grpcServer *grpc.Server
	jobMgr     *job.Manager
	registry   *registry.ComponentRegistry
	startTime  time.Time
}

// New() creates a new BeeRemoteServer that can be used with ListenAndServe().
// It requires a channel where it can send job requests to JobMgr.
func New(log *zap.Logger, config Config, jobMgr *job.Manager, buildInfo *flex.BuildInfo, features map[string]*flex.Feature) (*BeeRemoteServer, error) {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(BeeRemoteServer{}).PkgPath())))

	s := BeeRemoteServer{
		log:       log,
		Config:    config,
		wg:        new(sync.WaitGroup),
		jobMgr:    jobMgr,
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
	beeremote.RegisterBeeRemoteServer(s.grpcServer, &s)

	return &s, nil
}

// ListenAndServe should be called against a BeeRemoteServer initialized with New(). It spawns a new
// goroutine to handle serving requests until an an error occurs or Stop() is called against the
// BeeRemoteServer. It accepts an errChan where any errors will be returned if the gRPC server
// terminates early unexpectedly.
func (s *BeeRemoteServer) ListenAndServe(errChan chan<- error) {
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

// Stop should be called to gracefully terminate the server. It will stop the
// server then wait for outstanding RPCs to complete before returning.
func (s *BeeRemoteServer) Stop() {
	s.log.Info("attempting to stop gRPC server")
	s.grpcServer.Stop()
	s.wg.Wait()
}

func (s *BeeRemoteServer) SubmitJob(ctx context.Context, request *beeremote.SubmitJobRequest) (*beeremote.SubmitJobResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	var status beeremote.SubmitJobResponse_ResponseStatus = beeremote.SubmitJobResponse_CREATED
	result, err := s.jobMgr.SubmitJobRequest(request.GetRequest())
	if err != nil {
		if errors.Is(err, rst.ErrJobAlreadyComplete) {
			status = beeremote.SubmitJobResponse_ALREADY_COMPLETE
		} else if errors.Is(err, rst.ErrJobAlreadyOffloaded) {
			status = beeremote.SubmitJobResponse_ALREADY_OFFLOADED
		} else if errors.Is(err, rst.ErrJobAlreadyExists) {
			status = beeremote.SubmitJobResponse_EXISTING
		} else if errors.Is(err, rst.ErrJobFailedPrecondition) {
			status = beeremote.SubmitJobResponse_FAILED_PRECONDITION
		} else if errors.Is(err, rst.ErrJobNotAllowed) {
			status = beeremote.SubmitJobResponse_NOT_ALLOWED
		} else {
			return nil, err
		}
	}
	return beeremote.SubmitJobResponse_builder{
		Result: result,
		Status: status,
	}.Build(), nil
}

func (s *BeeRemoteServer) GetJobs(request *beeremote.GetJobsRequest, stream beeremote.BeeRemote_GetJobsServer) error {
	s.wg.Add(1)
	defer s.wg.Done()

	responses := make(chan *beeremote.GetJobsResponse, 1024)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
	sendResponses:
		for {
			resp, ok := <-responses
			if !ok {
				break sendResponses
			}
			// No reason to check the error because even if the stream breaks we need to continue
			// reading from the channel until it is closed. Otherwise the sender side may become
			// blocked permanently trying to send with no receiver if the channel is full and will
			// never get a chance to check if the context is cancelled.
			stream.Send(resp)
		}
	}()

	err := s.jobMgr.GetJobs(stream.Context(), request, responses)
	wg.Wait()
	if err != nil {
		if errors.Is(err, kvstore.ErrEntryNotInDB) {
			return status.Errorf(codes.NotFound, "%s", err.Error())
		}
		return err
	}
	return nil
}

func (s *BeeRemoteServer) UpdatePaths(request *beeremote.UpdatePathsRequest, stream beeremote.BeeRemote_UpdatePathsServer) error {
	s.wg.Add(1)
	defer s.wg.Done()

	responses := make(chan *beeremote.UpdatePathsResponse, 1024)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
	sendResponses:
		for {
			select {
			case <-stream.Context().Done():
				return
			case resp, ok := <-responses:
				if !ok {
					break sendResponses
				}
				stream.Send(resp)
			}
		}
	}()

	err := s.jobMgr.UpdatePaths(stream.Context(), request, responses)
	wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (s *BeeRemoteServer) UpdateJobs(ctx context.Context, request *beeremote.UpdateJobsRequest) (*beeremote.UpdateJobsResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	resp, err := s.jobMgr.UpdateJobs(request)
	if err != nil {
		if errors.Is(err, kvstore.ErrEntryNotInDB) {
			return nil, status.Errorf(codes.NotFound, "%s", err)
		}
		return nil, err
	}
	return resp, nil
}

func (s *BeeRemoteServer) GetRSTConfig(ctx context.Context, request *beeremote.GetRSTConfigRequest) (*beeremote.GetRSTConfigResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()

	rsts, err := s.jobMgr.GetRSTConfig()
	if err != nil {
		return nil, err
	}

	return beeremote.GetRSTConfigResponse_builder{
		Rsts: rsts,
	}.Build(), nil
}

func (s *BeeRemoteServer) UpdateWork(ctx context.Context, request *beeremote.UpdateWorkRequest) (*beeremote.UpdateWorkResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	return &beeremote.UpdateWorkResponse{}, s.jobMgr.UpdateWork(request.GetWork())
}

func (s *BeeRemoteServer) GetStubContents(ctx context.Context, request *beeremote.GetStubContentsRequest) (*beeremote.GetStubContentsResponse, error) {
	id, url, err := s.jobMgr.GetStubContents(request.Path)
	if err != nil {
		return &beeremote.GetStubContentsResponse{}, err
	}
	return &beeremote.GetStubContentsResponse{RstId: &id, Url: &url}, nil
}

func (s *BeeRemoteServer) GetCapabilities(ctx context.Context, request *flex.GetCapabilitiesRequest) (*flex.GetCapabilitiesResponse, error) {
	return &flex.GetCapabilitiesResponse{
		BuildInfo:      s.registry.GetBuildInfo(),
		Features:       s.registry.GetCapabilities(),
		StartTimestamp: timestamppb.New(s.startTime),
	}, nil
}
