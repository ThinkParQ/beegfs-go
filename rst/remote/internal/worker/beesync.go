package worker

import (
	"fmt"
	"net"
	"os"
	"strings"
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
}

var _ Worker = &BeeSyncNode{}
var _ grpcClientHandler = &BeeSyncNode{}

func newBeeSyncNode(baseNode *baseNode) Worker {
	beeSyncNode := &BeeSyncNode{baseNode: baseNode}
	beeSyncNode.baseNode.grpcClientHandler = beeSyncNode
	return beeSyncNode
}

func (n *BeeSyncNode) connect(config *flex.UpdateConfigRequest, bulkUpdate *flex.BulkUpdateWorkRequest, requiredFeatures map[string]*flex.Feature) (bool, error) {
	var cert []byte
	var err error
	if !n.config.TlsDisable && n.config.TlsCertFile != "" {
		cert, err = os.ReadFile(n.config.TlsCertFile)
		if err != nil {
			return false, fmt.Errorf("reading certificate file failed: %w", err)
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
		return false, err
	}

	n.client = flex.NewWorkerNodeClient(n.conn)
	config = proto.Clone(config).(*flex.UpdateConfigRequest)
	config.SetNodeId(n.GetID())

	// Verify the remote address is specified. If it's not specified (e.g. 0.0.0.0:9010) then use
	// a heartbeat rpc call to determine a valid address for the sync node to reach remote.
	host, port, err := net.SplitHostPort(config.BeeRemote.Address)
	if err != nil {
		return false, err
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsUnspecified() {
		var pr peer.Peer
		_, err := n.client.Heartbeat(n.nodeCtx, &flex.HeartbeatRequest{}, grpc.Peer(&pr))
		if err != nil {
			return true, fmt.Errorf("failed to determine remote address for sync node: %w", err)
		}

		if pr.LocalAddr == nil {
			return false, fmt.Errorf("failed to determine remote address for sync node")
		}

		host, _, err = net.SplitHostPort(pr.LocalAddr.String())
		if err != nil {
			return false, fmt.Errorf("failed to determine remote address for sync node: %w", err)
		}
		remoteAddr := net.JoinHostPort(host, port)
		n.log.Info("automatically determined address for sync node to communicate with remote", zap.String("remoteAddr", remoteAddr))
		config.BeeRemote.SetAddress(remoteAddr)
	}

	// Verify the sync node capabilities support remote.
	if clientRegistry, _, err := registry.GetComponentRegistry(n.rpcCtx, n.client); err != nil {
		return true, fmt.Errorf("failed to determine sync node capabilities: %w", err)
	} else if err = clientRegistry.RequireFeatures(requiredFeatures); err != nil {
		return true, fmt.Errorf("incompatible sync node: %w", err)
	}

	configureResp, err := n.client.UpdateConfig(n.rpcCtx, config)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			// TLS misconfiguration can cause a confusing error message so we handle it explicitly.
			// Note this is just a hint to the user, other error conditions may have the same
			// message so we don't adjust behavior (i.e., treat it as fatal).
			if strings.Contains(st.Message(), "error reading server preface: EOF") {
				return true, fmt.Errorf("%w (hint: check TLS is configured correctly on the client and server)", err)
			}
		}
		return true, err
	}

	// If we could send the message but the node didn't update the configuration
	// correctly probably we can't recover with a simple retry so consider fatal.
	if configureResp.GetResult() != flex.UpdateConfigResponse_SUCCESS {
		return false, fmt.Errorf("%s: node rejected config update: %s", configureResp.GetResult(), configureResp.GetMessage())
	}

	updateWRResp, err := n.client.BulkUpdateWork(n.rpcCtx, bulkUpdate)
	if err != nil {
		return true, err
	}

	// If we could send the message but the node couldn't update the WRs,
	// probably we can't recover with a simply retry so consider fatal.
	if !updateWRResp.GetSuccess() {
		return false, fmt.Errorf("bulk update of work requests on node failed with message %s", updateWRResp.GetMessage())
	}

	return false, nil
}

func (n *BeeSyncNode) heartbeat(request *flex.HeartbeatRequest) (*flex.HeartbeatResponse, error) {
	return n.client.Heartbeat(n.rpcCtx, request)
}

func (n *BeeSyncNode) disconnect() error {

	if n.conn != nil {
		if err := n.conn.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (n *BeeSyncNode) SubmitWork(request *flex.WorkRequest) (*flex.Work, error) {
	n.rpcWG.Add(1)
	defer n.rpcWG.Done()

	// Draining nodes are offered work as a last resort, so only an unreachable node is refused
	// outright here. A draining node may still decline, which is handled from its response below.
	if state := n.GetState(); state != ONLINE && state != DRAINING {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
	}

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
				// TODO: https://github.com/ThinkParQ/bee-remote/issues/39
				// Ideally don't panic here and figure out how to handle more gracefully.
				panic("work request already exists on node, this should never happen unless something is misconfigured or there is a new bug: " + err.Error())
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
		if n.GetState() != DRAINING {
			n.log.Info("node declined a work request because it is draining, further requests will only be assigned to it as a last resort")
			n.setState(DRAINING)
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
	n.rpcWG.Add(1)
	defer n.rpcWG.Done()

	if state := n.GetState(); state != ONLINE && state != DRAINING {
		return nil, fmt.Errorf("unable to update work request on a node that is %s", state)
	}

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
