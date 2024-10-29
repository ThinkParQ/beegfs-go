package pool

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/v8/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/v8/ctl/pkg/ctl/pool"
	pm "github.com/thinkparq/protobuf/go/management"
)

type deletePool_Config struct {
	pool    beegfs.EntityId
	execute bool
}

func newDeletePoolCmd() *cobra.Command {
	cfg := deletePool_Config{}

	cmd := &cobra.Command{
		Use:   "delete <pool>",
		Short: "Delete a storage pool.",
		Long: `Delete a storage pool.

WARNING: If this pool ID is still referenced in existing directory stripe patterns, creating files in that directory will fail until either the directory is updated or a new pool with the same ID is created. Note if you reuse the same pool ID in the future, it will automatically be used by any existing directories with that pool ID, which might be undesirable.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spp := beegfs.NewEntityIdParser(16, beegfs.Storage)
			p, err := spp.Parse(args[0])
			if err != nil {
				return err
			}
			cfg.pool = p

			return runDeletePoolCmd(cmd, cfg)
		},
	}

	cmd.Flags().BoolVar(&cfg.execute, "yes", false, "This command is destructive and by default runs in dry-run mode. Specify --yes for it to actually take action.")

	return cmd
}

func runDeletePoolCmd(cmd *cobra.Command, cfg deletePool_Config) error {
	resp, err := backend.Delete(cmd.Context(), &pm.DeletePoolRequest{
		Pool:    cfg.pool.ToProto(),
		Execute: &cfg.execute,
	})
	if err != nil {
		return err
	}

	res, err := beegfs.EntityIdSetFromProto(resp.Pool)
	if cfg.execute {
		if err != nil {
			fmt.Printf("Pool deleted, but received no id info from the server. Please verify the deletion using the `pool list` command.\n")
		} else {
			fmt.Printf("Pool deleted: %s\n", res)
		}
	} else {
		if err != nil {
			// Since it was a dry run, we report this error
			return fmt.Errorf("received no id info from the server")
		} else {
			fmt.Printf("Pool can be deleted: %s\nIf you really want to delete it, please add the --yes flag to the command.\n", res)
		}
	}

	return nil
}
