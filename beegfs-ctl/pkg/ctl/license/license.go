package license

import (
	"context"

	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/config"
	pl "github.com/thinkparq/protobuf/go/license"
	pm "github.com/thinkparq/protobuf/go/management"
)

// Get license information the management
func GetLicense(ctx context.Context, reload bool) (*pl.GetCertDataResult, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	license, err := mgmtd.GetLicense(ctx, &pm.GetLicenseRequest{Reload: &reload})
	if err != nil {
		return nil, err
	}

	return license.CertData, nil
}
