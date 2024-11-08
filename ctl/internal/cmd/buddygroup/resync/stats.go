package resync

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup/resync"
)

func newResyncStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats <buddy-group>",
		Short: "Retrieves statistics for running or completed resyncs.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			buddyGroup, err := beegfs.NewEntityIdParser(16, beegfs.Meta, beegfs.Storage).Parse(args[0])
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
		return fmt.Errorf("invalid target %s, only meta and storage targets are supported", primary)
	}

	return nil
}

func printMetaResults(result backend.MetaResyncStats_Result) {
	fmt.Println("Status")
	fmt.Println("------")
	fmt.Printf("Job State: %s\n", result.State)
	fmt.Printf("Start Time: %s\n", time.Unix(int64(result.StartTime), 0).Format(time.RFC3339))
	fmt.Printf("End Time: %s\n", time.Unix(int64(result.EndTime), 0).Format(time.RFC3339))

	fmt.Println("\nDiscovery Results")
	fmt.Println("-----------------")
	fmt.Printf("Directories Discovered: %d\n", result.DiscoveredDirs)
	fmt.Printf("Files Discovered: %d\n", result.GatherErrors)

	fmt.Println("\nSync Results")
	fmt.Println("------------")
	fmt.Printf("Directories Synced: %d\n", result.SyncedDirs)
	fmt.Printf("Files Synced: %d\n", result.SyncedFiles)

	fmt.Println("\nError Summary")
	fmt.Println("-------------")
	fmt.Printf("Directory Errors: %d\n", result.ErrorDirs)
	fmt.Printf("File Errors: %d\n", result.ErrorFiles)

	fmt.Println("\nSession Sync Details")
	fmt.Println("--------------------")
	fmt.Printf("Sessions to Sync: %d\n", result.SessionsToSync)
	fmt.Printf("Sessions Synced: %d\n", result.SyncedSessions)
	fmt.Printf("Session Sync Errors: %t\n", result.SessionSyncErrors)

	fmt.Println("\nModified Object Sync")
	fmt.Println("--------------------")
	fmt.Printf("Modified Objects Synced: %d\n", result.ModObjectsSynced)
	fmt.Printf("Modified Object Sync Errors: %d\n", result.ModSyncErrors)
}

func printStorageResults(result backend.StorageResyncStats_Result) {
	fmt.Println("Status")
	fmt.Println("------")
	fmt.Printf("Job State: %s\n", result.State)
	fmt.Printf("Start Time: %s\n", time.Unix(int64(result.StartTime), 0).Format(time.RFC3339))
	fmt.Printf("End Time: %s\n", time.Unix(int64(result.EndTime), 0).Format(time.RFC3339))

	fmt.Println("\nDiscovery Results")
	fmt.Println("-----------------")
	fmt.Printf("Files Discovered: %d\n", result.DiscoveredFiles)
	fmt.Printf("Directories Discovered: %d\n", result.DiscoveredDirs)

	fmt.Println("\nMatching Results")
	fmt.Println("----------------")
	fmt.Printf("Files Matched: %d\n", result.MatchedFiles)
	fmt.Printf("Directories Matched: %d\n", result.MatchedDirs)

	fmt.Println("\nSync Results")
	fmt.Println("------------")
	fmt.Printf("Files Synced: %d\n", result.SyncedFiles)
	fmt.Printf("Directories Synced: %d\n", result.SyncedDirs)

	fmt.Println("\nError Summary")
	fmt.Println("-------------")
	fmt.Printf("File Errors: %d\n", result.ErrorFiles)
	fmt.Printf("Directory Errors: %d\n", result.ErrorDirs)
}
