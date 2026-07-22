package resync

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup/resync"
)

func newResyncStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats <buddy-group>",
		Short: "Retrieve statistics for running or completed resyncs",
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

	outputType := config.OutputType(viper.GetString(config.OutputKey))

	switch primary.LegacyId.NodeType {
	case beegfs.Meta:
		result, err := backend.GetMetaResyncStats(cmd.Context(), primary)
		if err != nil {
			return err
		}
		if outputType.IsJSON() {
			return printResyncJSON(outputType, result)
		}
		printMetaResults(result)

	case beegfs.Storage:
		result, err := backend.GetStorageResyncStats(cmd.Context(), primary)
		if err != nil {
			return err
		}
		if outputType.IsJSON() {
			return printResyncJSON(outputType, result)
		}
		printStorageResults(result)

	default:
		return fmt.Errorf("invalid target %s, only meta and storage targets are supported", primary)
	}

	return nil
}

// printResyncJSON marshals a single resync-stats object to stdout. A resync report is a single
// object, so NDJSON is the same compact single line as JSON.
func printResyncJSON(outputType config.OutputType, v any) error {
	var (
		data []byte
		err  error
	)
	if outputType == config.OutputJSONPretty {
		data, err = json.MarshalIndent(v, "", " ")
	} else {
		data, err = json.Marshal(v)
	}
	if err != nil {
		return fmt.Errorf("marshaling resync stats: %w", err)
	}
	fmt.Println(string(data))
	return nil
}

func printMetaResults(result backend.MetaResyncStats) {
	fmt.Println("Status")
	fmt.Println("------")
	fmt.Printf("Job State: %s\n", result.State)
	startTime := result.StartTime
	if startTime == "" {
		startTime = "Not started"
	}
	fmt.Printf("Start Time: %s\n", startTime)
	if result.EndTime != "" {
		fmt.Printf("End Time: %s\n", result.EndTime)
	}

	fmt.Println("\nDiscovery Results")
	fmt.Println("-----------------")
	fmt.Printf("Directories Discovered: %d\n", result.DiscoveredDirs)
	fmt.Printf("Discovery Errors: %d\n", result.DiscoveryErrors)

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
	fmt.Printf("Modified Objects Synced: %d\n", result.ModifiedObjectsSynced)
	fmt.Printf("Modified Object Sync Errors: %d\n", result.ModifiedObjectSyncErrors)
}

func printStorageResults(result backend.StorageResyncStats) {
	fmt.Println("Status")
	fmt.Println("------")
	fmt.Printf("Job State: %s\n", result.State)
	startTime := result.StartTime
	if startTime == "" {
		startTime = "Not started"
	}
	fmt.Printf("Start Time: %s\n", startTime)
	if result.EndTime != "" {
		fmt.Printf("End Time: %s\n", result.EndTime)
	}

	fmt.Println("\nDiscovery Results")
	fmt.Println("-----------------")
	fmt.Printf("Files Discovered: %d\n", result.DiscoveredFiles)
	fmt.Printf("Directories Discovered: %d\n", result.DiscoveredDirs)

	fmt.Println("\nSync candidates")
	fmt.Println("---------------")
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
