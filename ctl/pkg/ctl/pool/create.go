package pool

import (
	"context"

	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func Create(ctx context.Context, req *pm.CreatePoolRequest) (*pm.CreatePoolResponse, error) {
	client, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.CreatePool(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
