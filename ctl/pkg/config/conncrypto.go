package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	switch protocol := strings.ToLower(viper.GetString(ProtocolKey)); protocol {
	case "", ProtocolLegacy:
		return nil, beecrypt.KeyPairID{}, nil
	case "plain":
		mode = beecrypt.ModePlain
	case "encrypted":
		mode = beecrypt.ModeEncrypted
	default:
		return nil, beecrypt.KeyPairID{}, fmt.Errorf("invalid --%s %q (valid: legacy, plain, encrypted)", ProtocolKey, protocol)
	}

	if viper.GetBool(TlsDisableKey) {
		return nil, beecrypt.KeyPairID{}, fmt.Errorf("--%s cannot be used with --%s %s, gRPC always uses TLS there", TlsDisableKey, ProtocolKey, viper.GetString(ProtocolKey))
	}

	mgmtdID, err := beecrypt.ParseKeyPairID(viper.GetString(MgmtdIdKey))
	if err != nil {
		return nil, beecrypt.KeyPairID{}, fmt.Errorf("--%s must be set to the management's key pair ID (sha256:<64 hex digits>): %w", MgmtdIdKey, err)
	}

	path, err := FindKeyFile()
	if err != nil {
		return nil, beecrypt.KeyPairID{}, err
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

const SystemKeyFile = "/etc/beegfs/key.pem"

func ProtocolIsLegacy() bool {
	protocol := strings.ToLower(viper.GetString(ProtocolKey))
	return protocol == "" || protocol == ProtocolLegacy
}

func UserKeyFile() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("determining the home directory: %w", err)
	}
	return filepath.Join(home, ".config", "beegfs", "key.pem"), nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func FindKeyFile() (string, error) {
	if path := viper.GetString(KeyFileKey); path != "" {
		if !fileExists(path) {
			return "", fmt.Errorf("key file %q does not exist, create it with \"beegfs auth generate-key %s\"", path, path)
		}
		return path, nil
	}

	userPath, err := UserKeyFile()
	if err == nil && fileExists(userPath) {
		return userPath, nil
	}
	if fileExists(SystemKeyFile) {
		return SystemKeyFile, nil
	}

	return "", fmt.Errorf("no key file found at %s or %s, create one with \"beegfs auth generate-key\" or set --%s", userPath, SystemKeyFile, KeyFileKey)
}

func GenerateKeyFile(path string) (string, beecrypt.KeyPairID, error) {
	if path == "" {
		path = viper.GetString(KeyFileKey)
	}
	if path == "" {
		var err error
		if path, err = UserKeyFile(); err != nil {
			return "", beecrypt.KeyPairID{}, err
		}
	}

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return "", beecrypt.KeyPairID{}, fmt.Errorf("creating directory for key file %q: %w", path, err)
	}

	pem, err := beecrypt.GenerateKeyPair()
	if err != nil {
		return "", beecrypt.KeyPairID{}, fmt.Errorf("generating key pair: %w", err)
	}
	defer clear(pem)

	keyPair, err := beecrypt.LoadKeyPair(pem)
	if err != nil {
		return "", beecrypt.KeyPairID{}, fmt.Errorf("loading generated key pair: %w", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", beecrypt.KeyPairID{}, fmt.Errorf("key file %q already exists", path)
		}
		return "", beecrypt.KeyPairID{}, fmt.Errorf("creating key file %q: %w", path, err)
	}
	if _, err := f.Write(pem); err != nil {
		f.Close()
		return "", beecrypt.KeyPairID{}, fmt.Errorf("writing key file %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", beecrypt.KeyPairID{}, fmt.Errorf("writing key file %q: %w", path, err)
	}

	return path, keyPair.ID(), nil
}

func EnsureKeyFile() (path string, generated bool, err error) {
	if path, err := FindKeyFile(); err == nil {
		return path, false, nil
	}

	path, _, err = GenerateKeyFile("")
	return path, err == nil, err
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
