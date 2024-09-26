package health

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"strings"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"go.uber.org/zap"
)

const (
	sysMgmtdHostKey = "sysMgmtdHost"
)

func printHeader(text string, char string) {
	fmt.Printf("%s", sPrintHeader(text, char))
}

func sPrintHeader(text string, char string) string {
	repeat := strings.Repeat(char, len(text))
	repeat = repeat[:len(text)]
	return fmt.Sprintf("%s\n%s\n%s\n", repeat, text, repeat)
}

// printClientHeader prints out a standard header before displaying content for various clients.
func printClientHeader(client procfs.Client, char string) {
	mgmtd, ok := client.Config[sysMgmtdHostKey]
	if !ok {
		mgmtd = "unknown"
	}
	printHeader(fmt.Sprintf("Client ID: %s (beegfs://%s@%s)", client.ID, mgmtd, client.Mount.Path), char)
}

// getFilteredClientList() gets the list of local BeeGFS client instances from procfs. It applies
// the following filters before returning the list:
//
//   - Filters out mounts for BeeGFS instances other than the management service configured for CTL.
//     Set noFilterByMgmtd to return all clients.
//   - If filterByMounts is specified, only the client(s) for those mount point(s) are returned.
func getFilteredClientList(ctx context.Context, noFilterByMgmtd bool, filterByMounts []string, backendCfg procfs.GetBeeGFSClientsConfig) ([]procfs.Client, error) {
	logger, _ := config.GetLogger()
	log := logger.With(zap.String("component", path.Base(reflect.TypeOf(checkCfg{}).PkgPath())))

	mounts := make(map[string]struct{})
	for _, arg := range filterByMounts {
		mounts[path.Clean(arg)] = struct{}{}
	}

	clients, err := procfs.GetBeeGFSClients(ctx, backendCfg)
	if err != nil {
		return nil, err
	}

	mgmtdAddrWithPort := viper.GetString(config.ManagementAddrKey)
	if !noFilterByMgmtd && mgmtdAddrWithPort == "" {
		return nil, fmt.Errorf("unable to proceed without a valid management service configured using '%s'", config.ManagementAddrKey)
	}
	mgmtAddr := strings.Split(mgmtdAddrWithPort, ":")[0]
	filteredClients := []procfs.Client{}

	for _, c := range clients {
		if !noFilterByMgmtd {
			foundMgmtdHost, ok := c.Config[sysMgmtdHostKey]
			if !ok {
				log.Debug("ignoring client mount because no management service (sysMgmtdHost) was found in its config", zap.Any("procDir", c.ProcDir))
				continue
			}
			if foundMgmtdHost != mgmtAddr {
				log.Debug(fmt.Sprintf("ignoring client mount because it is for a BeeGFS instance other than the one currently configured with CTL (use the %s flag to specify the other management service if this is unexpected)", config.ManagementAddrKey), zap.Any("procDir", c.ProcDir), zap.String("foundMgmtdService", foundMgmtdHost), zap.String("currentMgmtdService", mgmtAddr))
				continue
			}
		}
		if len(mounts) > 0 {
			if _, ok := mounts[c.Mount.Path]; !ok {
				log.Debug("ignoring client mount because it was not one of the user specified mount paths", zap.Any("procDir", c.ProcDir))
				continue
			}
		}
		filteredClients = append(filteredClients, c)
	}
	return filteredClients, nil
}
