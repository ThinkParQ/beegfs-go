package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

const dbUpgradeCmd = "db-upgrade"

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
		bflag.Flag("index-path", "", "Index directory path [default: IndexEnv.conf]", "-I", ""),
		bflag.Flag("db-version", "",
			"Upgrade/Downgrade database schema to the target Hive Index database version", "-T", 0),
		bflag.Flag("backup", "",
			"Backup database files while upgrading/downgrading database schema", "-b", false),
		bflag.Flag("delete", "",
			"Delete backup database files recursively from index directory path", "-d", false),
		bflag.Flag("restore", "",
			"Restore backup database files recursively from index directory path", "-r", false),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	err := cmd.MarkFlagRequired("db-version")
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return cmd
}

func newUpgradeCmd() *cobra.Command {
	s := newGenericUpgradeCmd()
	s.Use = "db-upgrade"
	s.Short = "BeeGFS Hive index database upgrade"
	s.Long = `Upgrade or Downgrade BeeGFS Hive Index database schema.

The BeeGFS Hive Index database upgrade utility is designed to update the
database schema to a specific BeeGFS Hive Index version. 
Only root users can upgrade the BeeGFS Hive Index database schema through SQL
scripts. SQL scripts default config directory is “/opt/beegfs/db”. 
If there are multiple SQL scripts present in the script directory then the
utility sorts all scripts in ascending order and executes one by one on each
database file.

Example: Upgrade to database version "2" through SQL script query config and
take backup before database upgrade
# beegfs index db-upgrade --db-version "2" --index-path /mnt/index --backup
`
	return s
}

func runPythonUpgradeIndex(bflagSet *bflag.FlagSet) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+2)
	allArgs = append(allArgs, dbUpgradeCmd, "-n", fmt.Sprint(viper.GetInt(config.NumWorkersKey)))
	allArgs = append(allArgs, wrappedArgs...)
	cmd := exec.Command(beeBinary, allArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("error starting command: %v", err)
	}
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("error executing beeBinary: %v", err)
	}
	return nil
}
