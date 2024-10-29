package rst

import (
	"context"

	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
)

type GetRSTCfg struct {
	ShowSecrets bool
}

func GetRSTConfig(ctx context.Context) (*beeremote.GetRSTConfigResponse, error) {
	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return nil, err
	}
	return beeRemote.GetRSTConfig(ctx, &beeremote.GetRSTConfigRequest{})
}
