package target

import (
	"fmt"

	"github.com/spf13/cobra"
	backend "github.com/thinkparq/beegfs-ctl/pkg/ctl/target"
	"github.com/thinkparq/gobee/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
)

type deleteTarget_Config struct {
	target  beegfs.EntityId
	execute bool
}

func newDeleteCmd() *cobra.Command {
	cfg := deleteTarget_Config{}

	cmd := &cobra.Command{
		Use:   "delete <target>",
		Short: "Delete a storage target.",
		Long: `Delete a storage target.

WARNING: If this target is still referenced in existing directory stripe patterns, creating files in that directory will fail until either the directory is updated or a new target with the same ID is created. Note if you reuse the same target ID in the future, it will automatically be used by any existing directories with that target ID, which might be undesirable.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spp := beegfs.NewEntityIdParser(16, beegfs.Storage)
			p, err := spp.Parse(args[0])
			if err != nil {
				return err
			}
			cfg.target = p

			return runDeleteCmd(cmd, cfg)
		},
	}

	cmd.Flags().BoolVar(&cfg.execute, "yes", false, "This command is destructive and by default runs in dry-run mode. Specify --yes for it to actually take action.")

	return cmd
}

func runDeleteCmd(cmd *cobra.Command, cfg deleteTarget_Config) error {
	resp, err := backend.Delete(cmd.Context(), &pm.DeleteTargetRequest{
		Target:  cfg.target.ToProto(),
		Execute: &cfg.execute,
	})
	if err != nil {
		return err
	}

	res, err := beegfs.EntityIdSetFromProto(resp.Target)
	if cfg.execute {
		if err != nil {
			fmt.Printf("Target deleted, but received no id info from the server. Please verify the deletion using the `target list` command.\n")
		} else {
			fmt.Printf("Target deleted: %s\n", res)
		}
	} else {
		if err != nil {
			// Since it was a dry run, we report this error
			return fmt.Errorf("Received no id info from the server.")
		} else {
			fmt.Printf(`Target can be deleted: %s
Deleting a target might cause adverse effects to your file system and should only be done if no files using it are left. If you really want to delete the target, please add the --yes flag to the command.
`, res)
		}
	}

	return nil
}
