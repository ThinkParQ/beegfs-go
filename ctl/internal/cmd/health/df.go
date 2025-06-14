package health

import (
	"context"
	"sort"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	tgtFrontend "github.com/thinkparq/beegfs-go/ctl/internal/cmd/target"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
)

func newDFCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:         "capacity",
		Aliases:     []string{"df"},
		Short:       "Show available disk space and inodes on metadata and storage targets (beegfs-df)",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := tgtBackend.GetTargets(cmd.Context())
			if err != nil {
				return err
			}
			printDF(cmd.Context(), targets, tgtFrontend.PrintConfig{Capacity: true})
			return nil
		},
	}
	return cmd
}

// printDF() is a wrapper for PrintTargetList() that prints metadata and storage targets as separate
// lists sorted by target ID.
func printDF(ctx context.Context, targets []tgtBackend.GetTargets_Result, printConfig tgtFrontend.PrintConfig) {
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
	tgtFrontend.PrintTargetList(ctx, printConfig, metaTargets)

	printHeader("Storage Targets", "-")
	tgtFrontend.PrintTargetList(ctx, printConfig, storageTargets)
}
