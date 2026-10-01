package target

import (
	"context"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func ResetRegistrationToken(ctx context.Context, req *pm.ResetTargetRegistrationTokenRequest) (*pm.ResetTargetRegistrationTokenResponse, error) {
	client, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.ResetTargetRegistrationToken(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, err
}
