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
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/registry"
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
	log *logger.Logger
	wg  *sync.WaitGroup
	Config
	grpcServer *grpc.Server
	jobMgr     *job.Manager
	registry   *registry.ComponentRegistry
	startTime  time.Time
}

// New() creates a new BeeRemoteServer that can be used with ListenAndServe().
// It requires a channel where it can send job requests to JobMgr.
func New(log *logger.Logger, config Config, jobMgr *job.Manager, buildInfo *flex.BuildInfo, features map[string]*flex.Feature) (*BeeRemoteServer, error) {

	log = log.With(zap.String("component", path.Base(reflect.TypeFor[BeeRemoteServer]().PkgPath())))

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

// GetGRPCServer returns the underlying gRPC server to enable additional services to be attached.
func (s *BeeRemoteServer) GetGRPCServer() *grpc.Server {
	return s.grpcServer
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

	result, err := s.jobMgr.SubmitJobRequest(request.GetRequest(), request.GetOriginNodeId())
	status, ok := job.SubmitResponseStatus(err)
	if !ok {
		return nil, err
	}
	return beeremote.SubmitJobResponse_builder{
		Result: result,
		Status: status,
	}.Build(), nil
}

func (s *BeeRemoteServer) GetJobs(request *beeremote.GetJobsRequest, stream beeremote.BeeRemote_GetJobsServer) error {
	s.wg.Add(1)
	defer s.wg.Done()

	streamCtx := stream.Context()
	responses := make(chan *beeremote.GetJobsResponse, 1024)

	wg := sync.WaitGroup{}
	wg.Go(func() {
	sendResponses:
		for streamCtx.Err() == nil {
			select {
			case <-streamCtx.Done():
			case resp, ok := <-responses:
				if !ok {
					break sendResponses
				}
				// No reason to check the error because even if the stream breaks we need to continue
				// reading from the channel until it is closed. Otherwise the sender side may become
				// blocked permanently trying to send with no receiver if the channel is full and will
				// never get a chance to check if the context is cancelled.
				stream.Send(resp)
			}
		}

		// Drain any remaining responses so sender is never blocked
		for range responses {
		}
	})

	err := s.jobMgr.GetJobs(streamCtx, request, responses)
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

	streamCtx := stream.Context()
	responses := make(chan *beeremote.UpdatePathsResponse, 1024)

	wg := sync.WaitGroup{}
	wg.Go(func() {
	sendResponses:
		for streamCtx.Err() == nil {
			select {
			case <-streamCtx.Done():
			case resp, ok := <-responses:
				if !ok {
					break sendResponses
				}
				stream.Send(resp)
			}
		}

		// Drain any remaining responses so sender is never blocked
		for range responses {
		}
	})

	err := s.jobMgr.UpdatePaths(streamCtx, request, responses)
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

	rsts, deleted := s.jobMgr.GetRSTConfig(request.GetIncludeDeleted())
	response := beeremote.GetRSTConfigResponse_builder{
		Rsts:    rsts,
		Deleted: deleted,
	}.Build()
	if request.GetIncludeDefinitions() {
		parents, definitions, err := s.jobMgr.GetRSTDefinitions()
		if err != nil {
			return nil, err
		}
		response.SetParents(parents)
		response.SetDefinitions(definitions)
	}
	return response, nil
}

func (s *BeeRemoteServer) CreateRST(ctx context.Context, request *beeremote.CreateRSTRequest) (*beeremote.CreateRSTResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	if !request.HasRst() {
		return nil, status.Error(codes.InvalidArgument, "the request has no remote storage target")
	}
	created, effective, apply, reusedUnfinished, err := s.jobMgr.CreateRST(ctx, request.GetRst(), request.GetForce())
	if err != nil {
		return nil, err
	}
	return beeremote.CreateRSTResponse_builder{Rst: created, Effective: effective, Apply: apply, ReusedUnfinishedJobs: reusedUnfinished}.Build(), nil
}

func (s *BeeRemoteServer) UpdateRST(ctx context.Context, request *beeremote.UpdateRSTRequest) (*beeremote.UpdateRSTResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	if !request.HasRst() || !request.HasExpected() {
		return nil, status.Error(codes.InvalidArgument, "the request needs the new and the expected remote storage target")
	}
	if request.GetRst().GetId() != request.GetExpected().GetId() {
		return nil, status.Error(codes.InvalidArgument, "the new and the expected remote storage target have different IDs")
	}
	effective, apply, err := s.jobMgr.UpdateRST(ctx, request.GetRst(), request.GetExpected(), request.GetForce())
	if err != nil {
		return nil, err
	}
	return beeremote.UpdateRSTResponse_builder{Apply: apply, Effective: effective}.Build(), nil
}

func (s *BeeRemoteServer) DeleteRST(ctx context.Context, request *beeremote.DeleteRSTRequest) (*beeremote.DeleteRSTResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	unfinished, apply, err := s.jobMgr.DeleteRST(ctx, request.GetId(), request.GetReason(), request.GetDryRun())
	if err != nil {
		return nil, err
	}
	return beeremote.DeleteRSTResponse_builder{UnfinishedJobs: unfinished, Apply: apply}.Build(), nil
}

func (s *BeeRemoteServer) CreateRSTParent(ctx context.Context, request *beeremote.CreateRSTParentRequest) (*beeremote.CreateRSTParentResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	if !request.HasParent() {
		return nil, status.Error(codes.InvalidArgument, "the request has no remote storage target parent")
	}
	created, err := s.jobMgr.CreateRSTParent(ctx, request.GetParent())
	if err != nil {
		return nil, err
	}
	return beeremote.CreateRSTParentResponse_builder{Parent: created}.Build(), nil
}

func (s *BeeRemoteServer) UpdateRSTParent(ctx context.Context, request *beeremote.UpdateRSTParentRequest) (*beeremote.UpdateRSTParentResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	if !request.HasParent() || !request.HasExpected() {
		return nil, status.Error(codes.InvalidArgument, "the request needs the new and the expected remote storage target parent")
	}
	apply, affected, err := s.jobMgr.UpdateRSTParent(ctx, request.GetName(), request.GetParent(), request.GetExpected(), request.GetForce())
	if err != nil {
		return nil, err
	}
	return beeremote.UpdateRSTParentResponse_builder{Apply: apply, AffectedTargets: affected}.Build(), nil
}

func (s *BeeRemoteServer) DeleteRSTParent(ctx context.Context, request *beeremote.DeleteRSTParentRequest) (*beeremote.DeleteRSTParentResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	if err := s.jobMgr.DeleteRSTParent(ctx, request.GetName()); err != nil {
		return nil, err
	}
	return &beeremote.DeleteRSTParentResponse{}, nil
}

func (s *BeeRemoteServer) UpdateWork(ctx context.Context, request *beeremote.UpdateWorkRequest) (*beeremote.UpdateWorkResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	return &beeremote.UpdateWorkResponse{}, s.jobMgr.UpdateWork(request.GetWork())
}

func (s *BeeRemoteServer) GetStubContents(ctx context.Context, request *beeremote.GetStubContentsRequest) (*beeremote.GetStubContentsResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	id, url, err := s.jobMgr.GetStubContents(request.Path)
	if err != nil {
		return &beeremote.GetStubContentsResponse{}, err
	}
	return &beeremote.GetStubContentsResponse{RstId: &id, Url: &url}, nil
}

func (s *BeeRemoteServer) GetCapabilities(ctx context.Context, request *flex.GetCapabilitiesRequest) (*flex.GetCapabilitiesResponse, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	return &flex.GetCapabilitiesResponse{
		BuildInfo:      s.registry.GetBuildInfo(),
		Features:       s.registry.GetCapabilities(),
		StartTimestamp: timestamppb.New(s.startTime),
	}, nil
}
