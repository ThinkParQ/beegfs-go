// Package iotest implements the "beegfs iotest" subcommand family.
//
// There are two groups of commands:
//
//   - Standalone tools: smoke, verify, dump, bench run/verify.
//     These operate on a single node without coordination files.
//
//   - Framework-managed workloads: soak, bench (via "beegfs iotest start"),
//     inval.  The framework handles SSH fan-out, per-node status files,
//     stop-signal propagation, and the start/status/stop lifecycle.
//     Each workload implements the Tool interface; the framework knows nothing
//     about what the tool actually does.
//
// Coordination files live under --path (default /tmp/beegfs-iotest) and use
// the naming convention .iotest-<workload>-<hostname>.json for status and
// .iotest.stop for the stop signal.  Using a shared BeeGFS path allows every
// node's status to be visible from any single node via "beegfs iotest status".
package iotest

import "github.com/spf13/cobra"

// helpIfNoFlags prints help and returns true when the command was invoked
// with no flags, signalling RunE to return immediately.
func helpIfNoFlags(cmd *cobra.Command) bool {
	if cmd.Flags().NFlag() == 0 {
		_ = cmd.Help()
		return true
	}
	return false
}

// NewIotestCmd returns the "beegfs iotest" command, with every standalone
// tool and framework-managed workload already registered as a subcommand.
func NewIotestCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "iotest",
		Short: "Verifiable IO testing tools for BeeGFS filesystems",
		Long: `Verifiable IO testing tools for BeeGFS filesystems.

Each write produces a block body that is deterministically generated from a
seed and self-checked via per-stripe CRC32C checksums; the block carries no
descriptive header. Instead, the header (seed, kind, node, offset, etc.) is
stored exclusively in an xattr on the same inode, never embedded in the
block data. A subsequent verify sweep can detect silent corruption, torn
writes, and stale data.`,
	}
	cmd.AddCommand(
		newSmokeCmd(),
		newVerifyCmd(),
		newDumpCmd(),
		newBenchCmd(),
	)
	cmd.AddCommand(NewFrameworkCmds(&SoakTool{}, &BenchTool{}, &InvalTool{})...)
	return cmd
}
