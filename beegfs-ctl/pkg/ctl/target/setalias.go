package target

import (
	"context"

	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/common/beegfs"
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
		EntityType: pb.EntityType_TARGET,
		NewAlias:   string(newAlias)})
	if err != nil {
		return err
	}

	return nil
}
