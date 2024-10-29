package buddygroup

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/ctl/buddygroup"
)

func newMirrorRootInodeCmd() *cobra.Command {
	var execute bool

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize mirroring for the root directory (this operation cannot be undone).",
		Long: `Initialize  mirroring for the root directory.

WARNING: This command cannot be undone. Please make sure that the following conditions are met before initializing root directory mirroring:
* Metadata server mirror groups have been created.
* All clients have been stopped.

If the root directory contains any files, those files will be mirrored as well. 
Subdirectories of the root directory will not be mirrored by this operation.

Mirroring for files can be enabled by moving them from an unmirrored directory to a mirrored directory.	   
		`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMirrorRootInode(cmd, execute)
		},
	}

	cmd.Flags().BoolVar(&execute, "yes", false, "This command cannot be undone and thus requires explicit confirmation. Specify --yes for it to actually take action.")

	return cmd
}

func runMirrorRootInode(cmd *cobra.Command, execute bool) error {

	if !execute {
		fmt.Printf(`Initialize mirroring for the root directory cannot be undone. 
If you really want to initialize mirroring for the root directory, please add the --yes flag to the command.
`)
		return nil
	}

	err := buddygroup.MirrorRootInode(cmd.Context())
	if err != nil {
		return err
	}

	fmt.Printf(`Successfully initialized mirroring for the root directory.
NOTE: To complete activating mirroring, please remount any clients and restart all metadata services now.
`)

	return nil
}
