package storagepool

import (
	"context"

	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/gobee/beegfs"
	pb "github.com/thinkparq/protobuf/go/beegfs"
)

func SetAlias(ctx context.Context, eid beegfs.EntityId, newAlias beegfs.Alias) error {
	client, err := config.ManagementClient()
	if err != nil {
		return err
	}

	eidp := eid.ToProto()
	if l := eidp.GetLegacyId(); l != nil {
		l.EntityType = pb.EntityType_STORAGE_POOL
	}

	_, err = client.SetAlias(ctx, &pb.SetAliasRequest{EntityId: eidp, NewAlias: string(newAlias)})
	if err != nil {
		return err
	}

	return nil
}
