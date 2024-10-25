package index

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

type upgradeIndexConfig struct {
	indexPath string
	dbVersion int
	backup    bool
	delete    bool
	restore   bool
}

func newGenericUpgradeCmd() *cobra.Command {
	cfg := upgradeIndexConfig{}

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonUpgradeIndex(&cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.indexPath, "index-path", "", "Index directory path [default: IndexEnv.conf]")
	cmd.Flags().IntVar(&cfg.dbVersion, "db-version", 0,
		"Upgrade/Downgrade database schema to the target Hive Index database version")
	cmd.Flags().BoolVar(&cfg.backup, "backup", false, "Backup database files while upgrading/downgrading database schema")
	cmd.Flags().BoolVar(&cfg.delete, "delete", false, "Delete backup database files recursively from index directory path")
	cmd.Flags().BoolVar(&cfg.restore, "restore", false, "Restore backup database files recursively from index directory path")
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

func runPythonUpgradeIndex(cfg *upgradeIndexConfig) error {
	args := []string{
		"db-upgrade",
	}

	if cfg.dbVersion != 0 {
		args = append(args, "-T", strconv.Itoa(cfg.dbVersion))
	}
	if cfg.indexPath != "" {
		args = append(args, "-I", cfg.indexPath)
	}
	if cfg.backup {
		args = append(args, "-b")
	}
	if cfg.delete {
		args = append(args, "-d")
	}
	if cfg.restore {
		args = append(args, "-r")
	}
	args = append(args, "-n", fmt.Sprint(viper.GetInt(config.NumWorkersKey)))

	cmd := exec.Command(beeBinary, args...)
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
