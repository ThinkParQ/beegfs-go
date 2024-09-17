package beegrpc

import (
	"context"
	"fmt"
	"time"

	pl "github.com/thinkparq/protobuf/go/license"
	pm "github.com/thinkparq/protobuf/go/management"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Mgmtd is a wrapper around the BeeGFS management's gRPC client. It is intended to provide common
// non-GRPC specific functionality such as verifying feature licensing.
type Mgmtd struct {
	pm.ManagementClient
	conn *grpc.ClientConn
}

func NewMgmtd(address string, connOpts ...connOpt) (*Mgmtd, error) {
	c, err := NewClientConn(address, connOpts...)
	if err != nil {
		return nil, err
	}
	return &Mgmtd{
		ManagementClient: pm.NewManagementClient(c),
		conn:             c,
	}, nil

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
		zap.Any("certificate", fmt.Sprintf("SP-%d", license.Data.Serial)),
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
			featureLicensed = true
			break
		}
	}
	if !featureLicensed {
		return licenseDetail, fmt.Errorf("the provided license does not include %s (licensed features: %+v)", requestedFeature, license.Data.DnsNames)
	}
	return licenseDetail, nil
}

func (m *Mgmtd) Cleanup() {
	if m.conn != nil {
		m.conn.Close()
	}
}
