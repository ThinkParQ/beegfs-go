package quota

import (
	"context"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

func SetDefault(ctx context.Context, req *pm.SetDefaultQuotaLimitsRequest) error {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return err
	}

	_, err = mgmtd.SetDefaultQuotaLimits(ctx, req)

	return err
}

func SetLimits(ctx context.Context, req *pm.SetQuotaLimitsRequest) error {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return err
	}

	_, err = mgmtd.SetQuotaLimits(ctx, req)

	return err
}

func GetLimits(ctx context.Context, req *pm.GetQuotaLimitsRequest) (pm.Management_GetQuotaLimitsClient, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	stream, err := mgmtd.GetQuotaLimits(ctx, req)
	if err != nil {
		return nil, err
	}

	return stream, err
}

func GetUsage(ctx context.Context, req *pm.GetQuotaUsageRequest) (pm.Management_GetQuotaUsageClient, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	stream, err := mgmtd.GetQuotaUsage(ctx, req)
	if err != nil {
		return nil, err
	}

	return stream, err
}
