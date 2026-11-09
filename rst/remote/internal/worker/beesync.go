package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/registry"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type BeeSyncNode struct {
	*baseNode
	conn   *grpc.ClientConn
	client flex.WorkerNodeClient
	// beeRemote is the BeeRemote address the last successful connect sent to the node. connect
	// resolves an unspecified listen address (such as 0.0.0.0) to the address the node can reach,
	// and UpdateConfig must send that same address, so the node keeps its connection to BeeRemote.
	// connect writes it from the Handle goroutine while UpdateConfig reads it from others.
	beeRemote atomic.Pointer[flex.BeeRemoteNode]
}

var _ Worker = &BeeSyncNode{}
var _ grpcClientHandler = &BeeSyncNode{}

func newBeeSyncNode(baseNode *baseNode) Worker {
	beeSyncNode := &BeeSyncNode{baseNode: baseNode}
	beeSyncNode.baseNode.grpcClientHandler = beeSyncNode
	return beeSyncNode
}

func (n *BeeSyncNode) connect(ctx context.Context, config *flex.UpdateConfigRequest, bulkUpdate *flex.BulkUpdateWorkRequest, requiredFeatures map[string]*flex.Feature) error {
	var cert []byte
	var err error
	if !n.config.TlsDisable && n.config.TlsCertFile != "" {
		cert, err = os.ReadFile(n.config.TlsCertFile)
		if err != nil {
			return fmt.Errorf("reading certificate file failed: %w", err)
		}
	}
	n.conn, err = beegrpc.NewClientConn(
		n.config.Address,
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithTLSDisableVerification(n.config.TLSDisableVerification),
		beegrpc.WithTLSDisable(n.config.TlsDisable),
		beegrpc.WithProxy(n.config.UseProxy),
	)
	if err != nil {
		return err
	}

	n.client = flex.NewWorkerNodeClient(n.conn)
	config = proto.Clone(config).(*flex.UpdateConfigRequest)
	config.SetNodeId(n.GetID())

	// Verify the remote address is specified. If it's not specified (e.g. 0.0.0.0:9010) then use
	// a heartbeat rpc call to determine a valid address for the sync node to reach remote.
	host, port, err := net.SplitHostPort(config.BeeRemote.Address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsUnspecified() {
		var pr peer.Peer
		_, err := n.client.Heartbeat(ctx, &flex.HeartbeatRequest{}, grpc.Peer(&pr))
		if err != nil {
			return fmt.Errorf("failed to determine remote address for sync node: %w", err)
		}

		if pr.LocalAddr == nil {
			return fmt.Errorf("failed to determine remote address for sync node")
		}

		host, _, err = net.SplitHostPort(pr.LocalAddr.String())
		if err != nil {
			return fmt.Errorf("failed to determine remote address for sync node: %w", err)
		}
		remoteAddr := net.JoinHostPort(host, port)
		n.log.Info("automatically determined address for sync node to communicate with remote", zap.String("remoteAddr", remoteAddr))
		config.BeeRemote.SetAddress(remoteAddr)
	}

	// Verify the sync node capabilities support remote.
	if clientRegistry, _, err := registry.GetComponentRegistry(ctx, n.client); err != nil {
		return fmt.Errorf("failed to determine sync node capabilities: %w", err)
	} else if err = clientRegistry.RequireFeatures(requiredFeatures); err != nil {
		return fmt.Errorf("incompatible sync node: %w", err)
	}

	configureResp, err := n.client.UpdateConfig(ctx, config)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			// TLS misconfiguration can cause a confusing error message so we handle it explicitly.
			// Note this is just a hint to the user. Other error conditions may have the same
			// message.
			if strings.Contains(st.Message(), "error reading server preface: EOF") {
				return fmt.Errorf("%w (hint: check TLS is configured correctly on the client and server)", err)
			}
		}
		return err
	}

	// The node received the config but rejected it. This usually needs an operator to fix the
	// configuration, but the caller still retries with backoff in case the node recovers.
	if configureResp.GetResult() != flex.UpdateConfigResponse_SUCCESS {
		return fmt.Errorf("%w: %s: %s", ErrConfigRejected, configureResp.GetResult(), configureResp.GetMessage())
	}

	updateWRResp, err := n.client.BulkUpdateWork(ctx, bulkUpdate)
	if err != nil {
		return err
	}

	// The node received the work requests but could not apply them. The caller retries with
	// backoff like any other connect error.
	if !updateWRResp.GetSuccess() {
		return fmt.Errorf("bulk update of work requests on node failed with message %s", updateWRResp.GetMessage())
	}

	n.beeRemote.Store(config.GetBeeRemote())
	return nil
}

func (n *BeeSyncNode) UpdateConfig(request *flex.UpdateConfigRequest) error {
	if _, ok := n.beginRPC(); !ok {
		return ErrNodeNotConnected
	}
	defer n.rpcWG.Done()

	request = proto.Clone(request).(*flex.UpdateConfigRequest)
	request.SetNodeId(n.GetID())
	if beeRemote := n.beeRemote.Load(); beeRemote != nil {
		request.SetBeeRemote(beeRemote)
	}

	// BeeRemote waits for every node before it answers the admin who made the change, so one node
	// that never answers must not hold that answer forever.
	ctx, cancel := context.WithTimeout(n.rpcCtx, connectTimeout)
	defer cancel()
	resp, err := n.client.UpdateConfig(ctx, request)
	if err == nil && resp.GetResult() != flex.UpdateConfigResponse_SUCCESS {
		err = fmt.Errorf("%w: %s: %s", ErrConfigRejected, resp.GetResult(), resp.GetMessage())
	}
	if err != nil {
		// The node may now hold a configuration BeeRemote no longer serves. Placing it offline
		// stops new work from reaching it, and the reconnect sends the current configuration.
		n.reportError(err)
		return err
	}
	return nil
}

// VerifyBackend sends backend to the node, which contacts the backend's store. The deadline comes
// from ctx, which BeeRemote sets per node. It is also cancelled when the node disconnects.
func (n *BeeSyncNode) VerifyBackend(ctx context.Context, backend *flex.RemoteStorageTarget) (string, error) {
	if _, ok := n.beginRPC(); !ok {
		return "", ErrNodeNotConnected
	}
	defer n.rpcWG.Done()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(n.rpcCtx, cancel)
	defer stop()
	resp, err := n.client.VerifyBackend(ctx, &flex.VerifyBackendRequest{Backend: backend})
	switch status.Code(err) {
	case codes.Unimplemented:
		return "", ErrVerifyUnsupported
	case codes.Unavailable:
		// The node is gone, such as a sync process that just stopped, but Remote has not noticed
		// yet and still counts it as connected. That says nothing about the backend, so it is
		// reported like any node that is not connected, not as a failed check.
		return "", fmt.Errorf("%w: %s", ErrNodeNotConnected, err)
	}
	if err != nil {
		return "", fmt.Errorf("the request to the node failed: %w", err)
	}
	if !resp.GetOk() {
		return "", errors.New(resp.GetMessage())
	}
	return resp.GetMessage(), nil
}

func (n *BeeSyncNode) heartbeat(ctx context.Context, request *flex.HeartbeatRequest) (*flex.HeartbeatResponse, error) {
	return n.client.Heartbeat(ctx, request)
}

func (n *BeeSyncNode) disconnect() error {
	if n.conn == nil {
		return nil
	}

	// Clear the connection even if Close fails. gRPC returns an error when closing a connection
	// that is already closed, so keeping it would make every later disconnect fail the same way.
	err := n.conn.Close()
	n.conn = nil
	return err
}

func (n *BeeSyncNode) SubmitWork(request *flex.WorkRequest) (*flex.Work, error) {
	// Draining nodes are offered work as a last resort, so only an unreachable node is refused
	// outright here. A draining node may still decline, which is handled from its response below.
	if _, ok := n.beginRPC(); !ok {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}
	defer n.rpcWG.Done()

	var resp *flex.SubmitWorkResponse
	var err error
	alreadyNotified := false
	for i := 0; i <= n.config.SendRetries; i++ {
		resp, err = n.client.SubmitWork(n.rpcCtx, flex.SubmitWorkRequest_builder{Request: request}.Build())
		if rpcStatus, ok := status.FromError(err); ok {
			// FailedPrecondition likely means the node is up but not yet ready. Most likely it
			// restarted and hasn't received any configuration from BeeRemote yet.
			if rpcStatus.Code() == codes.FailedPrecondition {
				if !alreadyNotified {
					// Report the error to the handler once so it will try to reconnect. Don't
					// report the error multiple times or it may cause unpredictable behavior.
					n.reportError(err)
				}
				time.Sleep(time.Duration(n.config.RetryInterval) * time.Second)
				continue
			} else if rpcStatus.Code() == codes.AlreadyExists {
				// The node already tracks this request. This happens when a job is submitted again
				// under the same job ID after remote lost the commit that recorded the first
				// submission, so the same work request reaches the node twice. The node keeps
				// running its copy and reports progress through UpdateWork, so the request is
				// treated as scheduled here.
				n.log.Warn("work request was already submitted to this node, treating it as scheduled", zap.String("jobId", request.GetJobId()), zap.String("requestId", request.GetRequestId()))
				return flex.Work_builder{
					Path:      request.GetPath(),
					JobId:     request.GetJobId(),
					RequestId: request.GetRequestId(),
					Status: flex.Work_Status_builder{
						State:   flex.Work_SCHEDULED,
						Message: "the node already had this work request",
					}.Build(),
				}.Build(), nil
			}
		}
		break
	}

	if err != nil {
		if !alreadyNotified {
			n.reportError(err)
		}
		return nil, err
	}

	// A node declines when it is too far into shutting down to record the request, and nodes
	// predating support for accepting while draining decline every request. Either way nothing was
	// created there, so the request is unassigned and may be offered elsewhere.
	if resp.GetStatus() == flex.SubmitWorkResponse_DRAINING {
		// Only an ONLINE node moves to DRAINING here. Handle may have placed the node offline
		// while this RPC ran, and OFFLINE must stand so the node is not offered work while it
		// reconnects.
		if n.compareAndSetState(ONLINE, DRAINING) {
			n.log.Info("node declined a work request because it is draining, further requests will only be assigned to it as a last resort")
		}
		return nil, ErrNodeDraining
	}

	// Callers treat a nil error as proof the request is now tracked on this node, and a result with
	// no status cannot be tracked or updated. Refusing it here keeps that contract from depending on
	// what a node chooses to put in its response.
	work := resp.GetWork()
	if work.GetStatus() == nil {
		return nil, fmt.Errorf("node accepted the work request but returned no work result")
	}

	return work, nil
}

func (n *BeeSyncNode) reportError(err error) {
	select {
	case n.rpcErr <- err:
	default:
	}
}

func (n *BeeSyncNode) UpdateWork(request *flex.UpdateWorkRequest) (*flex.Work, error) {
	state, ok := n.beginRPC()
	if !ok {
		return nil, fmt.Errorf("unable to update work request on a node that is %s", state)
	}
	defer n.rpcWG.Done()

	var resp *flex.UpdateWorkResponse
	var err error
	alreadyNotified := false
	for i := 0; i <= n.config.SendRetries; i++ {
		resp, err = n.client.UpdateWork(n.rpcCtx, request)
		if rpcStatus, ok := status.FromError(err); ok {
			// FailedPrecondition likely means the node is up but not yet ready. Most likely it
			// restarted and hasn't received any configuration from BeeRemote yet.
			if rpcStatus.Code() == codes.FailedPrecondition {
				if !alreadyNotified {
					// Report the error to the handler once so it will try to reconnect. Don't
					// report the error multiple times or it may cause unpredictable behavior.
					n.reportError(err)
				}
				time.Sleep(time.Duration(n.config.RetryInterval) * time.Second)
				continue
			} else if rpcStatus.Code() == codes.NotFound {
				return nil, ErrWorkRequestNotFound
			}
		}
		break
	}

	if err != nil {
		if !alreadyNotified {
			n.reportError(err)
		}
		return nil, err
	}
	return resp.GetWork(), nil
}
