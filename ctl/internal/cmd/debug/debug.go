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

Known commands for both meta and storage nodes:
  - "version": Print the node's service release version
  - "msgqueuestats": Print statistics about the node's message queues
  - "varlogmessages": Return contents of node's /var/log/messages file
  - "varlogkernlog": Return contents of node's /var/log/kern.log file
  - "beegfslog": Return contents of node's logStdFile
  - "loadavg": Print node's load average
  - "dropcaches": Drop all caches on the node by writing 3 to /proc/sys/vm/drop_caches
  - "getcfg": Return contents of the node's configuration file
  - "getloglevel": Print log levels for all log topics
  - "setloglevel <level> [<topic>]": Set the log level for <topic> (or all if no topic) to <level>
  - "net": Print the node's network connections
  - "quotaexceeded <target or pool id> <"uid"/"gid"> <"size"/"inode">": Print users (uid) or groups (gid) that exceeded their size or inode quotas on the given target or pool
  - "liststoragestates": List the node's last known state of all storage nodes
  - "setrejectionrate <rate>": Set the node's RDMA connection rejection rate to <rate>. Only for RDMA debugging!

Known meta node commands:
  - "listfileappendlocks <parentEntryID> <entryID> [<isBuddyMirrored>]": List all held append locks for entry <entryID> in directory <parentEntryID>
  - "listfileentrylocks <parentEntryID> <entryID> [<isBuddyMirrored>]": List all held entry locks for entry <entryID> in directory <parentEntryID>
  - "listfilerangelocks <parentEntryID> <entryID> [<isBuddyMirrored>]": List all held range locks for entry <entryID> in directory <parentEntryID>
  - "listopenfiles": Print all open file sessions for inodes owned by node
  - "refstats": Print numbers of referenced directories and files on node
  - "cachestats": Print the size of the node's directory reference cache
  - "listpools": Print information about capacity pool states
  - "dumpdentry parentEntryID> <entryName> [<isBuddyMirrored>]": Print information about the dentry <entryName> in directory <parentEntryID>
  - "dumpinode <inodeID> [<isBuddyMirrored>]": Print information about the inode <inodeID>
  - "dumpinlinedinode <parentEntryID> <entryName> [<isBuddyMirrored>]": Print information about the inlined inode <entryName> in directory <parentEntryID>
  - "liststoragestates": List the node's last known state of all storage nodes
  - "liststoragepools": List the node's last known information about storage pools
  Only available when node's service binary is a DEBUG build:
  - "writedirdentry <parentEntryID> <entryName> <ownerNodeID> [<isBuddyMirrored>]": Create a directory dentry <entryName> in parent directory <parentEntryID> who's inode is owned by <ownerNodeID>
  - "writedirinode <entryID> <parentDirID> <parentNodeID> <ownerNodeID> <mode> <uid> <gid> <size> <numLinks> [<isBuddyMirrored>]": Create a directory inode with the given attributes
  - "writefileinode <parentDirID> <entryName> <entryID> <mode> <uid> <gid> <filesize> <numLinks> <stripeTargets>": Create a file inode with the given attributes

Known storage node commands:
  - "listopenfiles": Print all open file sessions for chunk files owned by node
  - "resyncqueuelen <targetID> <"files"/"dirs">": Print statistics about the files or dirs resync queues for target <targetID>
  - "chunklockstoresize <targetID>": Print the size of target <targetID>'s chunk lock store
  - "chunklockstore <targetID> [<maxEntries>]": Print information about (at most <maxEntries>) entries in target <targetID>'s chunk lock store
  - "usedquota <target or pool ID> <"uid"/"gid"> [<forEachTarget>] range <fromID> <toID>": Print information about user (uid) or group (gid) quota utilization on target <targetID> for user/group IDs in range <fromID>-<toID>

These lists arte not guaranteed to be exhaustive and some commands might not be available under all circumstances and can change any time.
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
