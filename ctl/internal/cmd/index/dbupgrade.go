package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

const dbUpgradeCmd = "db"

func newGenericUpgradeCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonUpgradeIndex(bflagSet)
		},
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("index-path", "I", "BeeGFS Index directory path", "-I", ""),
		bflag.Flag("db-version", "T", "BeeGFS Hive index database upgrade to version", "-T", ""),
		bflag.Flag("upgrade", "U", "BeeGFS Hive Index database upgrade", "-U", false),
		bflag.Flag("downgrade", "D", "BeeGFS Hive Index database downgrade", "-D", false),
		bflag.Flag("backup", "b", "Backup Hive index database files before upgrading, allowing restoration if needed.", "-b", false),
		bflag.Flag("delete", "d", "Delete previously backed up database files from index directory tree", "-d", false),
		bflag.Flag("restore", "r", "Restore Backup database files from index directory tree", "-r", false),
		bflag.Flag("version", "v", "Prints BeeGFS Hive Index database version", "-v", false),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)

	return cmd
}

func newUpgradeCmd() *cobra.Command {
	s := newGenericUpgradeCmd()
	s.Use = "db"
	s.Short = "Upgrades or downgrades the BeeGFS Hive index database schema."

	s.Long = `Upgrade or downgrade the BeeGFS Hive Index database schema to a specified version.

The BeeGFS Hive Index database upgrade utility updates the database schema while
preserving existing data, ensuring compatibility with the latest index database version.
Example:
Get current BeeGFS Hive Index database version:
# beegfs index db -v
Upgrade to latest BeeGFS Hive Index database version:
# beegfs index db -U -b
Upgrade to BeeGFS Hive Index specific database version 3:
# beegfs index db -U -b -T 3
`
	return s
}

func runPythonUpgradeIndex(bflagSet *bflag.FlagSet) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+3)
	allArgs = append(allArgs, dbUpgradeCmd, "-n", fmt.Sprint(viper.GetInt(config.NumWorkersKey)))
	allArgs = append(allArgs, wrappedArgs...)
	log.Debug("Running BeeGFS Hive Index db command",
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("dbUpgradeCmd", dbUpgradeCmd),
		zap.Int("numWorkers", viper.GetInt(config.NumWorkersKey)),
		zap.Any("allArgs", allArgs),
	)
	cmd := exec.Command(beeBinary, allArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("unable to start index command: %w", err)
	}
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("error executing index command: %w", err)
	}
	return nil
}

// Can be removed once db Flag is enabled
var _ = newGenericUpgradeCmd
var _ = newUpgradeCmd
var _ = runPythonUpgradeIndex
