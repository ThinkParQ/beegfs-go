package index

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

type createStatsConfig struct {
	recursive    bool
	cumulative   bool
	order        string
	delim        string
	numResults   uint
	uid          string
	inMemoryName string
	stat         string
	path         string
	version      bool
}

var RECURSIVE = map[string]bool{
	"depth":      true,
	"filesize":   true,
	"filecount":  true,
	"linkcount":  true,
	"dircount":   true,
	"leaf-dirs":  true,
	"leaf-depth": true,
	"leaf-files": true,
	"leaf-links": true,
}

var CUMULATIVE = map[string]bool{
	"total-filesize":     true,
	"total-filecount":    true,
	"total-linkcount":    true,
	"total-dircount":     true,
	"total-leaf-files":   true,
	"total-leaf-links":   true,
	"files-per-level":    true,
	"links-per-level":    true,
	"dirs-per-level":     true,
	"average-leaf-files": true,
	"average-leaf-links": true,
}

var OTHERS = map[string]bool{
	"median-leaf-files": true,
	"duplicate-names":   true,
}

var validOrders = map[string]bool{
	"ASC":        true,
	"DESC":       true,
	"least":      true,
	"most":       true,
	"ASCENDING":  true,
	"DESCENDING": true,
}

var STATS = mergeMaps(RECURSIVE, CUMULATIVE, OTHERS)

func mergeMaps(maps ...map[string]bool) map[string]bool {
	merged := make(map[string]bool)
	for _, m := range maps {
		for k := range m {
			merged[k] = true
		}
	}
	return merged
}

func getStatChoices() string {
	keys := make([]string, 0, len(STATS))
	for k := range STATS {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}

func newGenericStatsCmd() *cobra.Command {
	cfg := createStatsConfig{}

	var cmd = &cobra.Command{
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			if len(args) < 1 {
				return fmt.Errorf("stat argument is required")
			}
			cfg.stat = args[0]
			if _, valid := STATS[cfg.stat]; !valid {
				return fmt.Errorf("invalid stat '%s', must be one of: %v", cfg.stat, getStatChoices())
			}
			if cfg.recursive && CUMULATIVE[cfg.stat] {
				return fmt.Errorf("--recursive/-r has no effect on \"%s\" statistic", cfg.stat)
			}
			if cfg.cumulative && RECURSIVE[cfg.stat] {
				return fmt.Errorf("--cumulative/-c has no effect on \"%s\" statistic", cfg.stat)
			}
			if (cfg.recursive || cfg.cumulative) && OTHERS[cfg.stat] {
				return fmt.Errorf("--recursive/-r and --cumulative/-c have no effect on \"%s\" statistic", cfg.stat)
			}

			if len(args) > 1 {
				cfg.path = args[1]
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
			return runPythonExecStats(&cfg)
		},
	}

	cmd.Flags().BoolVarP(&cfg.recursive, "recursive", "r", false, "Run command recursively")
	cmd.Flags().BoolVarP(&cfg.cumulative, "cumulative", "c", false, "Return cumulative values")
	cmd.Flags().StringVar(&cfg.order, "order", "ASC", "Sort output (if applicable)")
	cmd.Flags().StringVar(&cfg.delim, "delim", " ", "Delimiter separating output columns")
	cmd.Flags().UintVar(&cfg.numResults, "num-results", 0, "Limit the number of results")
	cmd.Flags().StringVar(&cfg.uid, "uid", "", "Restrict to user")
	cmd.Flags().StringVar(&cfg.uid, "user", "", "Restrict to user")
	cmd.Flags().BoolVarP(&cfg.version, "version", "v", false, "Version of the find command.")
	cmd.Flags().StringVar(&cfg.inMemoryName, "in-memory-name", "out", "In-memory name")
	cmd.MarkFlagsMutuallyExclusive("uid", "user")
	err := cmd.Flags().MarkHidden("in-memory-name")
	if err != nil {
		return nil
	}

	return cmd
}

func newStatsCmd() *cobra.Command {
	s := newGenericStatsCmd()
	s.Use = "stats"
	s.Short = "Calculates stats of index directory"
	s.Long = `Generate the index by walking the source directory.

The stats subcommand allows users to obtain filesystem statistics
like the total number of files, directories or links in the directory hierarchy,
files, directories, or links per level, maximum and minimum file sizes..

Example: Get the total number of files under a directory
$ beegfs index stats total-filecount

positional arguments:
  {depth,filesize,filecount,linkcount,dircount,leaf-dirs,leaf-depth,leaf-files,leaf-links,total-filesize,total-filecount,total-linkcount,total-dircount,total-leaf-files,total-leaf-links,files-per-level,links-per-level,dirs-per-level,average-leaf-files,average-leaf-links,median-leaf-files,duplicate-names}
`
	return s

}

func validateStatsInputs(cfg *createStatsConfig) error {
	if !validOrders[cfg.order] {
		return fmt.Errorf("invalid order: %s. Must be one of ASC, DESC, least, or most", cfg.order)
	}
	return nil
}

func runPythonExecStats(cfg *createStatsConfig) error {
	if err := validateStatsInputs(cfg); err != nil {
		return err
	}

	args := []string{
		"stats",
	}

	if cfg.stat != "" {
		args = append(args, cfg.stat)
	}
	if cfg.path != "" {
		args = append(args, cfg.path)
	}
	if cfg.recursive {
		args = append(args, "-r")
	}
	if cfg.cumulative {
		args = append(args, "-c")
	}
	if cfg.order != "" {
		args = append(args, "--order", cfg.order)
	}
	if cfg.delim != "" {
		args = append(args, "--delim", cfg.delim)
	}
	if cfg.numResults > 0 {
		args = append(args, "--num-results", fmt.Sprintf("%d", cfg.numResults))
	}
	if cfg.version {
		args = append(args, "--version")
	}
	if cfg.uid != "" {
		args = append(args, "--uid", cfg.uid)
	}
	if cfg.inMemoryName != "" {
		args = append(args, "--in-memory-name", cfg.inMemoryName)
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
