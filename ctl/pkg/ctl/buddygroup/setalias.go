package buddygroup

import (
	"context"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
)

func SetAlias(ctx context.Context, eid beegfs.EntityId, newAlias beegfs.Alias) error {
	client, err := config.ManagementClient()
	if err != nil {
		return err
	}

	eidp := eid.ToProto()

	_, err = client.SetAlias(ctx, &pm.SetAliasRequest{
		EntityId:   eidp,
		EntityType: pb.EntityType_BUDDY_GROUP,
		NewAlias:   string(newAlias)})
	if err != nil {
		return err
	}

	return nil
}
