package resync

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup/resync"
)

func newResyncStatsCmd() *cobra.Command {
	var buddyGroup beegfs.EntityId

	cmd := &cobra.Command{
		Use:   "stats <buddy-group>",
		Short: "Resync stats",
		Long:  "Resync stats",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			buddyGroup, err = beegfs.NewEntityIdParser(16, beegfs.Meta, beegfs.Storage).Parse(args[0])
			if err != nil {
				return err
			}

			return runResyncStatsCmd(cmd, &buddyGroup)
		},
	}

	return cmd
}

func runResyncStatsCmd(cmd *cobra.Command, buddyGroup *beegfs.EntityId) error {
	primary, err := backend.GetPrimaryTarget(cmd.Context(), *buddyGroup)
	if err != nil {
		return err
	}

	switch primary.LegacyId.NodeType {
	case beegfs.Meta:
		result, err := backend.GetMetaResyncStats(cmd.Context(), primary)
		if err != nil {
			return err
		}
		printMetaResults(result)

	case beegfs.Storage:
		result, err := backend.GetStorageResyncStats(cmd.Context(), primary)
		if err != nil {
			return err
		}
		printStorageResults(result)

	default:
		return fmt.Errorf("invalid target %s, Only meta and storage are supported", primary)
	}

	return nil
}

func printMetaResults(result backend.MetaResyncStats_Result) {
	defaultColumns := []string{"Metric", "Status/Value"}
	tbl := cmdfmt.NewTableWrapper(defaultColumns, defaultColumns)
	defer tbl.PrintRemaining()

	tbl.Row("Job State", result.State)
	tbl.Row("Start Time", time.Unix(int64(result.StartTime), 0).Format(time.RFC3339))
	tbl.Row("End Time", time.Unix(int64(result.EndTime), 0).Format(time.RFC3339))
	tbl.Row("Directories Discovered", result.DiscoveredDirs)
	tbl.Row("Files Discovered", result.GatherErrors)
	tbl.Row("Directories Synced", result.SyncedDirs)
	tbl.Row("Files Synced", result.SyncedFiles)
	tbl.Row("Directory Errors", result.ErrorDirs)
	tbl.Row("File Errors", result.ErrorFiles)
	tbl.Row("Sessions to Sync", result.SessionsToSync)
	tbl.Row("Sessions Synced", result.SyncedSessions)
	tbl.Row("Session Sync Errors", result.SessionSyncErrors)
	tbl.Row("Modified Objects Synced", result.ModObjectsSynced)
	tbl.Row("Modified Object Sync Errors", result.ModSyncErrors)
}

func printStorageResults(result backend.StorageResyncStats_Result) {
	defaultColumns := []string{"Metric", "Status/Value"}
	tbl := cmdfmt.NewTableWrapper(defaultColumns, defaultColumns)
	defer tbl.PrintRemaining()

	tbl.Row("Job State", result.State)
	tbl.Row("Start Time", time.Unix(int64(result.StartTime), 0).Format(time.RFC3339))
	tbl.Row("End Time", time.Unix(int64(result.EndTime), 0).Format(time.RFC3339))
	tbl.Row("Files Discovered", result.DiscoveredFiles)
	tbl.Row("Directories Discovered", result.DiscoveredDirs)
	tbl.Row("Files Matched", result.MatchedFiles)
	tbl.Row("Directories Matched", result.MatchedDirs)
	tbl.Row("Files Synced", result.SyncedFiles)
	tbl.Row("Directories Synced", result.SyncedDirs)
	tbl.Row("File Errors", result.ErrorFiles)
	tbl.Row("Directory Errors", result.ErrorDirs)
}
