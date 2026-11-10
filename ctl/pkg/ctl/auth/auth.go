package auth

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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
	KeyPairID         string
	IdentityID        int64
	Status            RegisterStatus
	Message           string
	AlreadyRegistered bool
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
		KeyPairID:         crypto.KeyPair.ID().String(),
		IdentityID:        resp.GetIdentityId(),
		Message:           resp.GetMessage(),
		AlreadyRegistered: resp.GetAlreadyRegistered(),
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

func GenerateToken(ctx context.Context, scope TokenScope, reusable bool, comment string) (string, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return "", err
	}

	req := &pm.GenerateIdentityTokenRequest{Reusable: &reusable}
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

type Identity struct {
	ID       int64
	Name     string
	Admin    bool
	Revoked  bool
	KeyIDs   []string
	Node     string
	TokenID  int64
	HasToken bool
}

func ListIdentities(ctx context.Context) ([]Identity, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := mgmtd.ListIdentities(ctx, &pm.ListIdentitiesRequest{})
	if err != nil {
		return nil, fmt.Errorf("listing identities: %w", err)
	}

	identities := make([]Identity, 0, len(resp.GetIdentities()))
	for _, i := range resp.GetIdentities() {
		identity := Identity{
			ID:       i.GetIdentityId(),
			Name:     i.GetName(),
			Admin:    i.GetAdmin(),
			Revoked:  i.GetRevoked(),
			KeyIDs:   i.GetKeyIds(),
			TokenID:  i.GetTokenId(),
			HasToken: i.TokenId != nil,
		}
		if i.GetNode() != nil {
			if node, err := beegfs.EntityIdSetFromProto(i.GetNode()); err == nil {
				identity.Node = fmt.Sprintf("%s (%s)", node.LegacyId, node.Alias)
			}
		}
		identities = append(identities, identity)
	}

	return identities, nil
}

func identitySelector(selector string) (*pm.RevokeIdentityRequest, error) {
	selector = strings.TrimSpace(selector)
	req := &pm.RevokeIdentityRequest{}

	switch {
	case strings.HasPrefix(selector, "sha256:"):
		req.KeyId = &selector
	case strings.HasPrefix(strings.ToLower(selector), "id:"):
		id, err := strconv.ParseInt(selector[3:], 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid identity ID %q", selector)
		}
		req.IdentityId = &id
	default:
		node, err := beegfs.NewEntityIdParser(32, beegfs.Client, beegfs.Meta, beegfs.Storage).Parse(selector)
		if err != nil {
			return nil, err
		}
		req.Node = node.ToProto()
	}

	return req, nil
}

func RevokeIdentity(ctx context.Context, selector string, unrevoke bool) (int64, error) {
	req, err := identitySelector(selector)
	if err != nil {
		return 0, err
	}
	req.Unrevoke = &unrevoke

	mgmtd, err := config.ManagementClient()
	if err != nil {
		return 0, err
	}

	resp, err := mgmtd.RevokeIdentity(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("revoking identity: %w", err)
	}
	return resp.GetIdentityId(), nil
}

type Token struct {
	ID         int64
	Scope      string
	Reusable   bool
	Revoked    bool
	Comment    string
	Created    time.Time
	Used       *time.Time
	Identities uint32
}

func ListTokens(ctx context.Context) ([]Token, error) {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := mgmtd.ListTokens(ctx, &pm.ListTokensRequest{})
	if err != nil {
		return nil, fmt.Errorf("listing identity tokens: %w", err)
	}

	tokens := make([]Token, 0, len(resp.GetTokens()))
	for _, t := range resp.GetTokens() {
		token := Token{
			ID:         t.GetTokenId(),
			Scope:      "client",
			Reusable:   t.GetReusable(),
			Revoked:    t.GetRevoked(),
			Comment:    t.GetComment(),
			Created:    time.Unix(t.GetCreatedAtUnix(), 0),
			Identities: t.GetIdentities(),
		}
		if t.GetScope() == pm.GenerateIdentityTokenRequest_SCOPE_ADMIN {
			token.Scope = "admin"
		}
		if t.UsedAtUnix != nil {
			used := time.Unix(t.GetUsedAtUnix(), 0)
			token.Used = &used
		}
		tokens = append(tokens, token)
	}

	return tokens, nil
}

func RevokeTokens(ctx context.Context, ids []int64) error {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return err
	}

	if _, err := mgmtd.RevokeTokens(ctx, &pm.RevokeTokensRequest{TokenIds: ids}); err != nil {
		return fmt.Errorf("revoking identity tokens: %w", err)
	}
	return nil
}
