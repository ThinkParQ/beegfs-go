package worker

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (n *BeeSyncNode) connect(config *flex.UpdateConfigRequest, bulkUpdate *flex.BulkUpdateWorkRequest) (bool, error) {
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
		return false, fmt.Errorf("%s configure update on node with message %s", configureResp.GetResult(), configureResp.GetMessage())
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
	if n.GetState() != ONLINE {
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
	return resp.GetWork(), nil
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
	if n.GetState() != ONLINE {
		return nil, fmt.Errorf("unable to submit work request to an offline node")
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
