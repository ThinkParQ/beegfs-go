package buddygroup

import (
	"context"

	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func Delete(ctx context.Context, req *pm.DeleteBuddyGroupRequest) (*pm.DeleteBuddyGroupResponse, error) {
	client, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.DeleteBuddyGroup(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, err
}
