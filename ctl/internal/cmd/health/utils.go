package health

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"github.com/thinkparq/protobuf/go/management"
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
	printHeader(fmt.Sprintf("Client ID: %s (beegfs://%s -> %s)", client.ID, mgmtd, client.Mount.Path), char)
}

// getFilteredClientList() gets the list of local BeeGFS client instances from procfs. It applies
// the following filters before returning the list:
//
//   - Filters out mounts for BeeGFS instances other than the management service configured for CTL.
//     Set noFilterByMgmtd to return all clients.
//   - If filterByMounts is specified, only the client(s) for those mount point(s) are returned.
func getFilteredClientList(ctx context.Context, noFilterByMgmtd bool, filterByMounts []string, backendCfg procfs.GetBeeGFSClientsConfig) ([]procfs.Client, error) {
	log, _ := config.GetLogger()

	mgmtdClient, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	// ctlFsUUID is only required when the user requested to return all client mounts and not filter
	// out any client mounts that don't belong to the management node currently configured with CTL.
	ctlFsUUID := ""
	if !noFilterByMgmtd {
		resp, err := mgmtdClient.GetNodes(ctx, &management.GetNodesRequest{})
		if err != nil {
			return nil, fmt.Errorf("unable to get file system UUID from the management node: %w", err)
		}
		if resp.FsUuid == nil {
			return nil, fmt.Errorf("unable to filter by management node: file system UUID received from the management node is unexpectedly nil (this is likely a bug elsewhere)")
		}
		if *resp.FsUuid == "" {
			return nil, fmt.Errorf("unable to filter by management node: file system UUID received from the management node is unexpectedly empty (this is likely a bug elsewhere)")
		}
		ctlFsUUID = *resp.FsUuid
		log.Debug("filtering client mounts for the management node configured with CTL", zap.Any("ctlFsUUID", ctlFsUUID))
	} else {
		log.Debug("not filtering by management node: user requested all client mounts be included")
	}

	mounts := make(map[string]struct{})
	for _, arg := range filterByMounts {
		mounts[path.Clean(arg)] = struct{}{}
	}

	clients, err := procfs.GetBeeGFSClients(ctx, backendCfg, log)
	if err != nil {
		return nil, err
	}

	filteredClients := []procfs.Client{}

	for _, c := range clients {
		// The ctlFsUUID should only be empty when noFilterByMgmtd is set.
		if ctlFsUUID != "" {
			if c.FsUUID != ctlFsUUID {
				log.Debug(fmt.Sprintf("ignoring client mount because it is for a BeeGFS instance other than the one currently configured with CTL (use the %s flag to specify a different management node if this is unexpected)", config.ManagementAddrKey), zap.Any("mountProcDir", c.ProcDir), zap.String("mountFsUUID", c.FsUUID), zap.Any("mountPath", c.Mount.Path))
				continue
			}
		}
		if len(mounts) > 0 {
			if _, ok := mounts[c.Mount.Path]; !ok {
				log.Debug("ignoring client mount because it was not one of the user specified mount paths", zap.Any("procDir", c.ProcDir), zap.Any("mountPath", c.Mount.Path))
				continue
			}
		}
		log.Debug("including client mount", zap.Any("mountProcDir", c.ProcDir), zap.String("mountFsUUID", c.FsUUID), zap.Any("mountPath", c.Mount.Path))
		filteredClients = append(filteredClients, c)
	}
	return filteredClients, nil
}
