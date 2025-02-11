package debug

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	dbg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/debug"
)

// Creates new "node" command
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "debug <node> <command>",
		Hidden: true,
		Short:  "Send various debug commands to meta or storage nodes",
		Long: `Send various debug commands to meta or storage nodes.

Known meta node commands: listfileappendlocks, listfileentrylocks, listfilerangelocks, listopenfiles, refstats, cachestats, version, msgqueuestats, listpools, dumpdentry, dumpinode, dumpinlinedinode, writedirdentry, writedirinode, writefileinode
Known storage node commands: listopenfiles, version, msgqueuestats, resyncqueuelen, chunklockstoresize, chunklockstore, setrejectionrate

These lists might be incomplete or outdated. Some commands might not be available under all circumstances. Some commands require arguments which will usually be pointed to when using the command.
Remember this is a debug tool and NOT meant for normal use. Using it might have unintended effects or even damage your file system.`,
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			node, err := beegfs.NewEntityIdParser(16, beegfs.Meta, beegfs.Storage).Parse(args[0])
			if err != nil {
				return err
			}

			// Combine positionals after the node argument into one string so no "" are needed
			command := ""
			for _, a := range args[1:] {
				command += a + " "
			}

			return runGenericDebugCmd(cmd, node, command)
		},
	}

	return cmd
}

func runGenericDebugCmd(cmd *cobra.Command, node beegfs.EntityId, command string) error {
	resp, err := dbg.GenericDebugCmd(cmd.Context(), node, command)
	if err != nil {
		return err
	}

	fmt.Println(resp)

	return nil
}
