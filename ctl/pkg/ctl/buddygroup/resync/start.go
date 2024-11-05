package resync

import (
	"context"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func StartResync(ctx context.Context, group beegfs.EntityId, timestampSec int64, restart bool) error {
	client, err := config.ManagementClient()
	if err != nil {
		return err
	}

	_, err = client.StartResync(ctx, &pm.StartResyncRequest{
		BuddyGroup: group.ToProto(),
		Timestamp:  &timestampSec,
		Restart:    &restart,
	})
	if err != nil {
		return err
	}

	return nil
}
