package chunkbalance

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/chunkbalance"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
)

type chunkBalanceCfg struct {
	SourceID       uint16
	DestinationID  uint16
	Verbose        bool
	NullDelim      bool
	Mirrored       bool
	ReadFromStdin  bool
	UseMountedPath bool
	stdinDelimiter string
}

func NewCmd() *cobra.Command {
	chunkBalanceCfg := chunkBalanceCfg{}
	cmd := &cobra.Command{
		Use: "chunkbalance --sourceid=<sourceID> --destinationid=<destinationID> <path>",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		Short:       "Balance chunks between storage targets",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Long: `Balance chunks between storage targets.
SPECIFYING PATHS:
The <path> is required and can be a file or directory.
You can specify the --sourceid and --destinationid to indicate the storage targets.
The command can read multiple paths from stdin by using '-' as the <path> (e.g., 'cat path_list.txt | beegfs chunkbalance --sourceid=3 --destinationid=5 -').
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChunkBalanceCmd(cmd, args, chunkBalanceCfg)
		},
	}

	cmd.Flags().Uint16Var(&chunkBalanceCfg.SourceID, "sourceid", 0, "Balance chunks from the given source ID.")
	cmd.Flags().Uint16Var(&chunkBalanceCfg.DestinationID, "destinationid", 0, "Balance chunks to the given destination ID.")
	cmd.Flags().BoolVar(&chunkBalanceCfg.Verbose, "verbose", false, "Print verbose messages information.")
	cmd.Flags().BoolVar(&chunkBalanceCfg.NullDelim, "null", false, "Assume that paths on stdin are delimited by a null character instead of newline.")
	cmd.Flags().BoolVar(&chunkBalanceCfg.Mirrored, "mirrored", false, "Option to specify if the targetIDs are mirror buddy group IDs.")
	cmd.Flags().BoolVar(&chunkBalanceCfg.UseMountedPath, "unmounted", false, "Specify relative paths to the root directory of a possibly unmounted BeeGFS.")
	cmd.Flags().StringVar(&chunkBalanceCfg.stdinDelimiter, "stdin-delimiter", "\n", "Change the string delimiter used to determine individual paths when read from stdin (e.g., --stdin-delimiter=\"\\x00\" for NULL).")

	return cmd
}

func runChunkBalanceCmd(cmd *cobra.Command, args []string, chunkBalanceCfg chunkBalanceCfg) error {

	method, err := util.DeterminePathInputMethod(args, false, chunkBalanceCfg.stdinDelimiter)

	if err != nil {
		return err
	}

	getEntriesCfg := entry.GetEntriesCfg{}

	// both these parameters need to be true to get the full info about the chunk path
	getEntriesCfg.IncludeOrigMsg = true
	getEntriesCfg.Verbose = true

	entriesChan, errChan, err := entry.GetEntries(cmd.Context(), method, getEntriesCfg)
	if err != nil {
		return err
	}

	var multiErr types.MultiError

run:
	// Iterate over the entries received from entriesChan
	for {
		select {
		case entryInfo, ok := <-entriesChan:
			if !ok {
				break run
			}
			entryVar := entryInfo

			if chunkBalanceCfg.Mirrored {
				groups, err := buddygroup.GetBuddyGroups(cmd.Context())
				if err != nil {
					return err
				}
				if len(groups) == 0 {
					return fmt.Errorf("no mirrors configured")
				}
				foundSrc := false
				foundDst := false

				for _, group := range groups {
					if group.NodeType == beegfs.Storage {
						if strings.Contains(group.BuddyGroup.String(), fmt.Sprintf("%d", chunkBalanceCfg.SourceID)) {
							foundSrc = true
						}
						if strings.Contains(group.BuddyGroup.String(), fmt.Sprintf("%d", chunkBalanceCfg.DestinationID)) {
							foundDst = true
						}
					}
				}

				if !foundSrc {
					return fmt.Errorf("mirrored option used but sourceID is not a valid mirrorGroupID")
				}

				if !foundDst {
					return fmt.Errorf("mirrored option used but destinationID is not a valid mirrorGroupID")
				}
			} else {
				// Validation logic for source and destination IDs
				sourceIDValid, err := isValidTargetID(cmd.Context(), chunkBalanceCfg.SourceID)
				if err != nil {
					return fmt.Errorf("error validating sourceID (%d): %w", chunkBalanceCfg.SourceID, err)
				}
				if !sourceIDValid {
					return fmt.Errorf("sourceID (%d) is NOT a target in the file system", chunkBalanceCfg.SourceID)
				}

				destinationIDValid, err := isValidTargetID(cmd.Context(), chunkBalanceCfg.DestinationID)
				if err != nil {
					return fmt.Errorf("error validating destinationID (%d): %w", chunkBalanceCfg.DestinationID, err)
				}
				if !destinationIDValid {
					return fmt.Errorf("destinationID (%d) is NOT a target in the file system", chunkBalanceCfg.DestinationID)
				}

				// Check if source and destination IDs are in the pattern of the file
				sourceIDInPattern := isIDInPattern(entryVar.Entry.Pattern.TargetIDs, chunkBalanceCfg.SourceID)
				if !sourceIDInPattern {
					return fmt.Errorf("sourceID (%d) is NOT a target for the required path", chunkBalanceCfg.SourceID)
				}

				destinationIDInPattern := isIDInPattern(entryVar.Entry.Pattern.TargetIDs, chunkBalanceCfg.DestinationID)
				if destinationIDInPattern {
					return fmt.Errorf("destinationID (%d) is already a target for the required path", chunkBalanceCfg.DestinationID)
				}
			}

			err_chunkbalance := chunkbalance.ExecuteChunkBalance(cmd.Context(), entryVar, chunkBalanceCfg.SourceID, chunkBalanceCfg.DestinationID)
			if err_chunkbalance != nil {
				fmt.Println("Chunk balance failed:", err)
			}

		case err, ok := <-errChan:
			if ok {

				multiErr.Errors = append(multiErr.Errors, err)
			}
		}
	}
	return nil
}

func isIDInPattern(targetIDs []uint16, userSpecifiedID uint16) bool {
	for _, v := range targetIDs {
		if v == userSpecifiedID {
			return true
		}
	}
	return false
}

func isValidTargetID(ctx context.Context, targetID uint16) (bool, error) {
	storageTargets, err := tgtBackend.GetTargets(ctx)
	if err != nil {
		return false, err
	}

	for _, tgt := range storageTargets {
		if uint16(tgt.Target.LegacyId.NumId) == targetID {
			return true, nil
		}
	}
	return false, nil
}
