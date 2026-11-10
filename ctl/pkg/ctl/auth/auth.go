package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

type RegisterStatus int

const (
	RegisterOk RegisterStatus = iota
	RegisterPending
)

type RegisterResult struct {
	KeyPairID  string
	IdentityID int64
	Status     RegisterStatus
	Message    string
}

func Register(ctx context.Context, name string, token string) (RegisterResult, error) {
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
	if token != "" {
		req.IdentityToken = &token
	}

	resp, err := mgmtd.RegisterIdentity(ctx, req)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("registering identity at management: %w", err)
	}

	res := RegisterResult{
		KeyPairID:  crypto.KeyPair.ID().String(),
		IdentityID: resp.GetIdentityId(),
		Message:    resp.GetMessage(),
	}

	switch resp.GetStatus() {
	case pm.RegisterIdentityResponse_STATUS_OK:
		res.Status = RegisterOk
	case pm.RegisterIdentityResponse_STATUS_PENDING:
		res.Status = RegisterPending
	case pm.RegisterIdentityResponse_STATUS_REJECTED:
		return res, fmt.Errorf("management rejected the registration of key pair %s: %s", res.KeyPairID, res.Message)
	default:
		return res, fmt.Errorf("management returned an unknown registration status %v", resp.GetStatus())
	}

	return res, nil
}

type TokenScope int

const (
	ClientScope TokenScope = iota
	AdminScope
)

func GenerateToken(ctx context.Context, scope TokenScope, permanent bool, comment string) (string, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return "", err
	}

	req := &pm.GenerateIdentityTokenRequest{Permanent: &permanent}
	switch scope {
	case ClientScope:
		req.Scope = pm.GenerateIdentityTokenRequest_SCOPE_CLIENT.Enum()
	case AdminScope:
		req.Scope = pm.GenerateIdentityTokenRequest_SCOPE_ADMIN.Enum()
	}
	if comment != "" {
		req.Comment = &comment
	}

	resp, err := mgmtd.GenerateIdentityToken(ctx, req)
	if err != nil {
		return "", fmt.Errorf("generating identity token: %w", err)
	}

	return resp.GetToken(), nil
}

type PendingCategory int

const (
	ClientCategory PendingCategory = iota
	ServerCategory
)

func (c PendingCategory) String() string {
	if c == ServerCategory {
		return "server"
	}
	return "client"
}

type PendingKey struct {
	KeyPairID string
	Category  PendingCategory
	NodeType  beegfs.NodeType
	NodeID    uint32
	Name      string
	Addr      string
	Rejected  bool
	FirstSeen time.Time
	LastSeen  time.Time
}

func (k PendingKey) ClaimedNode() string {
	if k.NodeType == beegfs.InvalidNodeType {
		return ""
	}
	if k.NodeID == 0 {
		return fmt.Sprintf("new %s", k.NodeType)
	}
	return fmt.Sprintf("%s:%d", k.NodeType, k.NodeID)
}

func GetPending(ctx context.Context) ([]PendingKey, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := mgmtd.GetPendingKeys(ctx, &pm.GetPendingKeysRequest{})
	if err != nil {
		return nil, fmt.Errorf("requesting pending keys: %w", err)
	}

	keys := make([]PendingKey, 0, len(resp.GetKeys()))
	for _, k := range resp.GetKeys() {
		key := PendingKey{
			KeyPairID: k.GetKeyId(),
			Category:  ClientCategory,
			NodeType:  beegfs.InvalidNodeType,
			NodeID:    k.GetNodeId(),
			Name:      k.GetName(),
			Addr:      k.GetAddr(),
			Rejected:  k.GetRejected(),
			FirstSeen: time.Unix(k.GetFirstSeenUnix(), 0),
			LastSeen:  time.Unix(k.GetLastSeenUnix(), 0),
		}
		if k.GetCategory() == pm.GetPendingKeysResponse_PendingKey_CATEGORY_SERVER {
			key.Category = ServerCategory
		}
		if k.NodeType != nil {
			key.NodeType = beegfs.NodeTypeFromProto(k.GetNodeType())
		}
		keys = append(keys, key)
	}

	return keys, nil
}

func Conflicting(keys []PendingKey) map[string]bool {
	claims := map[string][]string{}
	for _, k := range keys {
		if k.Rejected || k.NodeType == beegfs.InvalidNodeType || k.NodeID == 0 {
			continue
		}
		node := k.ClaimedNode()
		claims[node] = append(claims[node], k.KeyPairID)
	}

	conflicting := map[string]bool{}
	for _, ids := range claims {
		if len(ids) > 1 {
			for _, id := range ids {
				conflicting[id] = true
			}
		}
	}
	return conflicting
}

func Approve(ctx context.Context, keyPairIDs []string) error {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return err
	}

	if _, err := mgmtd.ApproveKeys(ctx, &pm.ApproveKeysRequest{KeyIds: keyPairIDs}); err != nil {
		return fmt.Errorf("approving key pair IDs: %w", err)
	}
	return nil
}

func Reject(ctx context.Context, keyPairIDs []string) error {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return err
	}

	if _, err := mgmtd.RejectKeys(ctx, &pm.RejectKeysRequest{KeyIds: keyPairIDs}); err != nil {
		return fmt.Errorf("rejecting key pair IDs: %w", err)
	}
	return nil
}
