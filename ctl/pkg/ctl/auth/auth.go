package auth

import (
	"context"
	"fmt"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

type RegisterResult struct {
	KeyPairID  string
	IdentityID int64
}

func Register(ctx context.Context, name string) (RegisterResult, error) {
	crypto, _, err := config.ConnCrypto()
	if err != nil {
		return RegisterResult{}, err
	}
	if crypto == nil {
		return RegisterResult{}, fmt.Errorf("registering an identity requires --%s plain or encrypted", config.ConnProtocolKey)
	}

	mgmtd, err := config.UncheckedManagementClient()
	if err != nil {
		return RegisterResult{}, err
	}
	defer mgmtd.Cleanup()

	req := &pm.RegisterIdentityRequest{}
	if name != "" {
		req.Name = &name
	}

	resp, err := mgmtd.RegisterIdentity(ctx, req)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("registering identity at management: %w", err)
	}

	return RegisterResult{
		KeyPairID:  crypto.KeyPair.ID().String(),
		IdentityID: resp.GetIdentityId(),
	}, nil
}
