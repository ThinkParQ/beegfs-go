package node

import (
	"context"

	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func Delete(ctx context.Context, req *pm.DeleteNodeRequest) (*pm.DeleteNodeResponse, error) {
	client, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.DeleteNode(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, err
}
