package rst

import (
	"context"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
)

func GetStubContents(ctx context.Context, path string) (*beeremote.GetStubContentsResponse, error) {
	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return nil, err
	}
	return beeRemote.GetStubContents(ctx, &beeremote.GetStubContentsRequest{Path: path})
}
