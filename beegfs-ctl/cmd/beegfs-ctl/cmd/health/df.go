package health

import (
	"sort"

	"github.com/spf13/cobra"
	tgtFrontend "github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd/target"
	tgtBackend "github.com/thinkparq/beegfs-ctl/pkg/ctl/target"
	"github.com/thinkparq/gobee/beegfs"
)

func newDFCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "capacity",
		Aliases: []string{"df"},
		Short:   "Show available disk space and inodes on metadata and storage targets (beegfs-df).",
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := tgtBackend.GetTargets(cmd.Context())
			if err != nil {
				return err
			}
			printDF(targets, tgtFrontend.PrintConfig{Capacity: true})
			return nil
		},
	}
	return cmd
}

// printDF() is a wrapper for PrintTargetList() that prints metadata and storage targets as separate
// lists sorted by target ID.
func printDF(targets []tgtBackend.GetTargets_Result, printConfig tgtFrontend.PrintConfig) {
	metaTargets := []tgtBackend.GetTargets_Result{}
	storageTargets := []tgtBackend.GetTargets_Result{}
	for _, tgt := range targets {
		if tgt.NodeType == beegfs.Meta {
			metaTargets = append(metaTargets, tgt)
		} else if tgt.NodeType == beegfs.Storage {
			storageTargets = append(storageTargets, tgt)
		}
	}
	sort.Slice(metaTargets, func(i, j int) bool {
		return metaTargets[i].Target.LegacyId.NumId <= metaTargets[j].Target.LegacyId.NumId
	})
	sort.Slice(storageTargets, func(i, j int) bool {
		return storageTargets[i].Target.LegacyId.NumId <= storageTargets[j].Target.LegacyId.NumId
	})

	printHeader("Metadata Targets", "-")
	tgtFrontend.PrintTargetList(printConfig, metaTargets)

	printHeader("Storage Targets", "-")
	tgtFrontend.PrintTargetList(printConfig, storageTargets)
}
