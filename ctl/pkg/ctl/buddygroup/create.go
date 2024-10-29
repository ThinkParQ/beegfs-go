package buddygroup

import (
	"context"

	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func Create(ctx context.Context, req *pm.CreateBuddyGroupRequest) (*pm.CreateBuddyGroupResponse, error) {
	client, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.CreateBuddyGroup(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
