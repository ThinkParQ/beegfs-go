package buddygroup

import (
	"fmt"

	"github.com/spf13/cobra"
	backend "github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/ctl/buddygroup"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
)

type deleteBuddyGroup_Config struct {
	group   beegfs.EntityId
	execute bool
}

func newDeleteBuddyGroupCmd() *cobra.Command {
	cfg := deleteBuddyGroup_Config{}

	cmd := &cobra.Command{
		Use:   "delete <group>",
		Short: "Delete a storage buddy group.",
		Long: `Delete a storage buddy group.
WARNING: Incorrect use of this command can corrupt your system. Please make sure that one of the following conditions is met before removing a group:

* The filesystem is completely empty, with no files and no disposal files.
* The filesystem is offline (no mounted clients) and the last operations done with the filesystem were:
   * Migrate everything off of the group you want to remove.
   * Check whether disposal is empty (if not, abort).
   * Unmount the client used for migration.
`,
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spp := beegfs.NewEntityIdParser(16, beegfs.Storage)
			g, err := spp.Parse(args[0])
			if err != nil {
				return err
			}
			cfg.group = g

			return runDeleteBuddyGroupCmd(cmd, cfg)
		},
	}

	cmd.Flags().BoolVar(&cfg.execute, "yes", false, "This command is destructive and by default runs in dry-run mode. Specify --yes for it to actually take action.")

	return cmd
}

func runDeleteBuddyGroupCmd(cmd *cobra.Command, cfg deleteBuddyGroup_Config) error {
	resp, err := backend.Delete(cmd.Context(), &pm.DeleteBuddyGroupRequest{
		Group:   cfg.group.ToProto(),
		Execute: &cfg.execute,
	})
	if err != nil {
		return err
	}

	res, err := beegfs.EntityIdSetFromProto(resp.Group)
	if cfg.execute {
		if err != nil {
			fmt.Printf("Buddy group deleted, but received no id info from the server. Please verify the deletion using the `mirror list` command.\n")
		} else {
			fmt.Printf("Buddy group deleted: %s\n", res)
		}
	} else {
		if err != nil {
			// Since it was a dry run, we report this error
			return fmt.Errorf("received no id info from the server")
		} else {
			fmt.Printf(`Buddy group can be deleted: %s
Deleting a buddy group might cause adverse effects to your file system and should only be done if no files using it are left. If you really want to delete the buddy group, please add the --yes flag to the command.
`, res)
		}
	}

	return nil
}
