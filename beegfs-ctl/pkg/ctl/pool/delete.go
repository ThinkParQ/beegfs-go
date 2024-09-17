package pool

import (
	"context"

	"github.com/thinkparq/beegfs-ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func Delete(ctx context.Context, req *pm.DeletePoolRequest) (*pm.DeletePoolResponse, error) {
	client, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.DeletePool(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, err
}
