package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-crypto/go/beecrypt"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/beemsg/util"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
)

var (
	connCryptoOnce    sync.Once
	connCrypto        *util.ConnCrypto
	connCryptoMgmtdID beecrypt.KeyPairID
	connCryptoErr     error
)

func ConnCrypto() (*util.ConnCrypto, beecrypt.KeyPairID, error) {
	connCryptoOnce.Do(func() {
		connCrypto, connCryptoMgmtdID, connCryptoErr = loadConnCrypto()
	})
	return connCrypto, connCryptoMgmtdID, connCryptoErr
}

func loadConnCrypto() (*util.ConnCrypto, beecrypt.KeyPairID, error) {
	var mode beecrypt.Mode
	switch protocol := strings.ToLower(viper.GetString(ConnProtocolKey)); protocol {
	case "", ConnProtocolLegacy:
		return nil, beecrypt.KeyPairID{}, nil
	case "plain":
		mode = beecrypt.ModePlain
	case "encrypted":
		mode = beecrypt.ModeEncrypted
	default:
		return nil, beecrypt.KeyPairID{}, fmt.Errorf("invalid --%s %q (valid: legacy, plain, encrypted)", ConnProtocolKey, protocol)
	}

	if viper.GetBool(TlsDisableKey) {
		return nil, beecrypt.KeyPairID{}, fmt.Errorf("--%s cannot be used with --%s %s, gRPC always uses TLS there", TlsDisableKey, ConnProtocolKey, viper.GetString(ConnProtocolKey))
	}

	mgmtdID, err := beecrypt.ParseKeyPairID(viper.GetString(ConnMgmtdIdKey))
	if err != nil {
		return nil, beecrypt.KeyPairID{}, fmt.Errorf("--%s must be set to the management's key pair ID (sha256:<64 hex digits>): %w", ConnMgmtdIdKey, err)
	}

	path := viper.GetString(ConnKeyFileKey)
	if path == "" {
		return nil, beecrypt.KeyPairID{}, fmt.Errorf("--%s is required with --%s %s", ConnKeyFileKey, ConnProtocolKey, viper.GetString(ConnProtocolKey))
	}

	if viper.GetBool(ConnKeyGenerateKey) {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			if err := writeNewKeyFile(path); err != nil {
				return nil, beecrypt.KeyPairID{}, err
			}
		}
	}

	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, beecrypt.KeyPairID{}, fmt.Errorf("reading key file %q: %w", path, err)
	}
	keyPair, err := beecrypt.LoadKeyPair(pem)
	clear(pem)
	if err != nil {
		return nil, beecrypt.KeyPairID{}, fmt.Errorf("loading key file %q: %w", path, err)
	}

	return &util.ConnCrypto{KeyPair: keyPair, Mode: mode}, mgmtdID, nil
}

func writeNewKeyFile(path string) error {
	pem, err := beecrypt.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("generating key pair: %w", err)
	}
	defer clear(pem)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("creating key file %q: %w", path, err)
	}
	if _, err := f.Write(pem); err != nil {
		f.Close()
		return fmt.Errorf("writing key file %q: %w", path, err)
	}
	return f.Close()
}

func downloadPeerIDs(ctx context.Context, mgmtd *beegrpc.Mgmtd) (map[beegfs.LegacyId]beecrypt.KeyPairID, error) {
	resp, err := mgmtd.GetIdentities(ctx, &pm.GetIdentitiesRequest{})
	if err != nil {
		return nil, fmt.Errorf("downloading identities from management: %w", err)
	}

	peerIDs := make(map[beegfs.LegacyId]beecrypt.KeyPairID)
	for _, key := range resp.GetKeys() {
		legacyID := key.GetNode().GetLegacyId()
		if legacyID == nil {
			continue
		}

		var nodeType beegfs.NodeType
		switch legacyID.GetNodeType() {
		case pb.NodeType_META:
			nodeType = beegfs.Meta
		case pb.NodeType_STORAGE:
			nodeType = beegfs.Storage
		case pb.NodeType_CLIENT:
			nodeType = beegfs.Client
		case pb.NodeType_MANAGEMENT:
			nodeType = beegfs.Management
		default:
			continue
		}

		id, err := beecrypt.ParseKeyPairID(key.GetKeyId())
		if err != nil {
			return nil, fmt.Errorf("management sent an invalid key pair ID %q: %w", key.GetKeyId(), err)
		}
		peerIDs[beegfs.LegacyId{NumId: beegfs.NumId(legacyID.GetNumId()), NodeType: nodeType}] = id
	}

	return peerIDs, nil
}
