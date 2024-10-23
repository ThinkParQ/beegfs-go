package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

type statIndexConfig struct {
	beegfs bool
	path   string
}

func newGenericStatCmd() *cobra.Command {
	cfg := statIndexConfig{}

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				cfg.path = args[0]
			} else {
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				beegfsClient, err := config.BeeGFSClient(cwd)
				if err != nil {
					return err
				}
				cfg.path = beegfsClient.GetMountPath()
			}
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonStatIndex(&cfg)
		},
	}

	cmd.Flags().BoolVar(&cfg.beegfs, "beegfs", false, "Print BeeGFS Metadata for the File")

	return cmd
}

func newStatCmd() *cobra.Command {
	s := newGenericStatCmd()
	s.Use = "stat"
	s.Short = "Display file status"
	s.Long = `Displays file and directory metadata information.

Example: View the Stat of a File
$ beegfs index stat README

View the BeeGFS Stat of a File
$ beegfs index stat --beegfs README
`
	return s

}

func runPythonStatIndex(cfg *statIndexConfig) error {
	args := []string{
		"stat",
	}

	if cfg.beegfs {
		args = append(args, "--beegfs")
	}
	if cfg.path != "" {
		args = append(args, cfg.path)
	}
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
