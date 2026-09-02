package buddygroup

import (
	"context"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func Modify(ctx context.Context, req *pm.ModifyBuddyGroupRequest) (*pm.ModifyBuddyGroupResponse, error) {
	client, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.ModifyBuddyGroup(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
