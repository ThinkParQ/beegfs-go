package internal

import (
	"context"
	"fmt"

	"github.com/thinkparq/protobuf/go/beegfs"
)

// Get the complete list of nodes from the mananagement
func GetNodeList(ctx context.Context) (*beegfs.GetNodeListResp, error) {
	mgmtd, err := ManagementClient(ctx)
	if err != nil {
		return nil, err
	}

	res, err := mgmtd.GetNodeList(ctx, &beegfs.GetNodeListReq{})
	if err != nil {
		return nil, fmt.Errorf("Requesting node list failed: %w\n", err)
	}

	return res, nil
}

// Get info for the specified node from the management
func GetNodeInfo(ctx context.Context, alias string) (*beegfs.GetNodeInfoResp, error) {
	mgmtd, err := ManagementClient(ctx)
	if err != nil {
		return nil, err
	}

	res, err := mgmtd.GetNodeInfo(ctx, &beegfs.GetNodeInfoReq{Id: &beegfs.GetNodeInfoReq_Alias{Alias: alias}})
	if err != nil {
		return nil, fmt.Errorf("Requesting node info failed: %w\n", err)
	}

	return res, nil
}
