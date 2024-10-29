package pool

import (
	"context"

	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func Assign(ctx context.Context, req *pm.AssignPoolRequest) (*pm.AssignPoolResponse, error) {
	client, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.AssignPool(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
