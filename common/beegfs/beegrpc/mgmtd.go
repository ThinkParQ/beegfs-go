package beegrpc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/thinkparq/beegfs-go/common/beemsg/util"
	pl "github.com/thinkparq/protobuf/go/license"
	pm "github.com/thinkparq/protobuf/go/management"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Mgmtd is a wrapper around the BeeGFS management's gRPC client. It is intended to provide common
// non-GRPC specific functionality such as verifying feature licensing.
type Mgmtd struct {
	pm.ManagementClient
	conn       *grpc.ClientConn
	address    string
	authSecret []byte
}

func NewMgmtd(address string, connOpts ...connOpt) (*Mgmtd, error) {
	c, err := NewClientConn(address, connOpts...)
	if err != nil {
		return nil, err
	}
	return &Mgmtd{
		ManagementClient: pm.NewManagementClient(c),
		conn:             c,
		address:          address,
		authSecret:       applyConnOpts(connOpts...).AuthSecret,
	}, nil

}

func (m *Mgmtd) GetAuthSecret() uint64 {
	if m.authSecret != nil {
		return util.GenerateAuthSecret(m.authSecret)
	}
	return uint64(0)
}

func (m *Mgmtd) GetAuthSecretBytes() []byte {
	if m.authSecret != nil {
		b := make([]byte, len(m.authSecret))
		copy(b, m.authSecret)
		return b
	}
	return nil
}

// grandfatheredFeatures is used to handle when we introduce new licensed features that should be
// available by default in all licenses certificates but won't appear explicitly in license
// certificates generated before a certain point in time.
var grandfatheredFeatures map[string]time.Time = map[string]time.Time{
	"io.beegfs.rebalancing": time.Date(2025, 11, 1, 00, 00, 00, 00, time.UTC),
}

// VerifyLicense is a wrapper for GetLicenseRequest() that verifies the requested feature is defined
// in a valid license file installed to this mgmtd service. It returns simplified license details
// similar to runLicenseCmd() but in a format suitable for logging. If the license is not valid or
// the requested feature is not licensed an error will be returned. It will always attempt to return
// license details when available even if an error occurs. The caller should check if []zap.Field is
// nil to determine if license details are available.
func (m *Mgmtd) VerifyLicense(ctx context.Context, requestedFeature string) ([]zap.Field, error) {
	reload := false
	resp, err := m.GetLicense(ctx, &pm.GetLicenseRequest{Reload: &reload})
	if err != nil {
		return nil, fmt.Errorf("error downloading license from the BeeGFS management service: %w", err)
	}

	license := resp.CertData
	if license.Result == pl.VerifyResult_VERIFY_ERROR {
		return nil, fmt.Errorf("error verifying license: %s", license.Message)
	}

	licenseDetail := []zap.Field{
		zap.Any("certificate", license.Data.CommonName),
		zap.Any("licensedTo", license.Data.Organization),
		zap.Any("viaPartner", license.Data.ParentData.Organization),
		zap.Any("validFrom", license.Data.ValidFrom.AsTime().Add(14*time.Hour).Format("2006-01-02")),
		zap.Any("validUntil", license.Data.ValidUntil.AsTime().Add(-12*time.Hour).Format("2006-01-02")),
	}

	if license.Result == pl.VerifyResult_VERIFY_INVALID {
		return licenseDetail, fmt.Errorf("the provided license is invalid: %s", license.Message)
	}

	featureLicensed := false
	for _, gotFeature := range license.Data.DnsNames {
		if gotFeature == requestedFeature {
			os.Setenv("BEEGFS_LICENSED_FEATURE", requestedFeature)
			featureLicensed = true
			break
		}
	}
	if !featureLicensed {
		if featureIntroduced, ok := grandfatheredFeatures[requestedFeature]; ok {
			if license.Data.ValidFrom.AsTime().Before(featureIntroduced) {
				licenseDetail = append(licenseDetail, zap.Any("grandfatheredFeature", requestedFeature))
				os.Setenv("BEEGFS_LICENSED_FEATURE", requestedFeature)
				return licenseDetail, nil
			}
		}
		return licenseDetail, fmt.Errorf("the provided license does not include %s (licensed features: %+v)", requestedFeature, license.Data.DnsNames)
	}
	return licenseDetail, nil
}

// Returns the address:port of the configured management gRPC client.
func (m *Mgmtd) GetAddress() string {
	return m.address
}

func (m *Mgmtd) Cleanup() {
	if m.conn != nil {
		m.conn.Close()
	}
}

func (m *Mgmtd) GetFsUUID(ctx context.Context) (string, error) {
	resp, err := m.GetNodes(ctx, &pm.GetNodesRequest{})
	if err != nil {
		return "", fmt.Errorf("unable to get file system UUID from the management node: %w", err)
	}
	if resp.FsUuid == nil {
		return "", fmt.Errorf("file system UUID received from the management node is unexpectedly nil (this is likely a bug elsewhere)")
	}
	if *resp.FsUuid == "" {
		return "", fmt.Errorf("file system UUID received from the management node is unexpectedly empty (this is likely a bug elsewhere)")
	}
	return *resp.FsUuid, nil
}
