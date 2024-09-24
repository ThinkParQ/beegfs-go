package cmd

import (
	"fmt"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

var (
	BinaryName = "beegfs"
	Version    = "local-build"
	Commit     = "unknown"
	BuildTime  = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the command line tool version.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Version: %s | Commit %s | Built: %s\n", Version, Commit, BuildTime)
		fmt.Printf("\nHint: The version displayed here is that of the command line tool, which may differ from BeeGFS itself.\n")

		if viper.GetBool(config.DebugKey) {
			fmt.Println("\nDebug Info:")
			euid := syscall.Geteuid()
			uid := syscall.Getuid()
			egid := syscall.Getegid()
			gid := syscall.Getgid()
			fmt.Printf("* The binary was invoked with the following permissions: euid %d | uid %d | egid %d | gid %d\n", euid, uid, egid, gid)
		}
	},
}
