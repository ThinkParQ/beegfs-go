package index

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeindex"
)

var (
	updaterMu      sync.Mutex
	updaterClients = map[string]beeindex.IndexUpdaterClient{}
)

// UpdaterClient returns a client for the beegfs-index-updater service at cfg.UpdaterAddr. It uses
// the CLI's TLS settings and the management auth secret, as the beegfs-remote client does. One
// connection is kept per address. A failed attempt is not kept, so the next call tries again.
func UpdaterClient(cfg GlobalCfg) (beeindex.IndexUpdaterClient, error) {
	if cfg.UpdaterAddr == "" {
		return nil, errors.New("index updater address not set: pass --updater-addr or configure \"updater\" in .beegfs.index at the BeeGFS mount root")
	}
	updaterMu.Lock()
	defer updaterMu.Unlock()
	if c, ok := updaterClients[cfg.UpdaterAddr]; ok {
		return c, nil
	}
	var cert []byte
	if !viper.GetBool(config.TlsDisableKey) && viper.GetString(config.TlsCertFile) != "" {
		var err error
		if cert, err = os.ReadFile(viper.GetString(config.TlsCertFile)); err != nil {
			return nil, fmt.Errorf("reading certificate file failed: %w", err)
		}
	}
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}
	conn, err := beegrpc.NewClientConn(cfg.UpdaterAddr,
		beegrpc.WithTLSDisable(viper.GetBool(config.TlsDisableKey)),
		beegrpc.WithTLSDisableVerification(viper.GetBool(config.TlsDisableVerificationKey)),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithAuthSecret(mgmtd.GetAuthSecretBytes()),
		beegrpc.WithProxy(viper.GetBool(config.UseProxyKey)),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to beegfs-index-updater at %s: %w", cfg.UpdaterAddr, err)
	}
	c := beeindex.NewIndexUpdaterClient(conn)
	updaterClients[cfg.UpdaterAddr] = c
	return c, nil
}
