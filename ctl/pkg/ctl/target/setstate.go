package target

import (
	"context"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func SetState(ctx context.Context, req *pm.SetTargetStateRequest) error {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return err
	}

	_, err = mgmtd.SetTargetState(ctx, req)
	if err != nil {
		return err
	}

	return err
}
