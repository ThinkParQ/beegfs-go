package health

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

const (
	bundleExtension = ".tar.gz"
)

type supportFile struct {
	name     string
	contents []supportCommand
}

type supportCommand struct {
	sectionName string
	command     []string
}

// Note --debug is automatically set on all commands.
var supportBundleContents = []supportFile{
	{
		name: "overview.txt",
		contents: []supportCommand{
			{
				sectionName: "Version",
				command:     []string{"version"},
			},
			{
				sectionName: "Licensing",
				command:     []string{"license", "show"},
			},
		},
	}, {
		name: "health-check.txt",
		contents: []supportCommand{
			{
				sectionName: "Health Check",
				command:     []string{"health", "check", "--ignore-failed-checks", "--print-net", "--print-df"},
			},
		},
	}, {
		name: "nodes.txt",
		contents: []supportCommand{
			{
				sectionName: "Metadata Nodes",
				command:     []string{"node", "list", "--with-nics", "--reachability-check", "--node-type=meta"},
			},
			{
				sectionName: "Storage Nodes",
				command:     []string{"node", "list", "--with-nics", "--reachability-check", "--node-type=storage"},
			},
			{
				sectionName: "Clients",
				command:     []string{"node", "list", "--with-nics", "--reachability-check", "--node-type=storage"},
			},
		},
	}, {
		name: "targets.txt",
		contents: []supportCommand{
			{
				sectionName: "Metadata Targets",
				command:     []string{"target", "list", "--node-type=meta"},
			},
			{
				sectionName: "Storage Targets",
				command:     []string{"target", "list", "--node-type=storage"},
			},
		},
	}, {
		name: "mirrors.txt",
		contents: []supportCommand{
			{
				sectionName: "Metadata Mirrors",
				command:     []string{"mirror", "list", "--node-type=meta"},
			},
			{
				sectionName: "Storage Mirrors",
				command:     []string{"mirror", "list", "--node-type=storage"},
			},
		},
	}, {
		name: "pools.txt",
		contents: []supportCommand{
			{
				sectionName: "Storage Pools",
				command:     []string{"pool", "list", "--with-limits"},
			},
		},
	},
	{
		name: "remote-storage-targets.txt",
		contents: []supportCommand{
			{
				sectionName: "Remote Storage Targets",
				command:     []string{"rst", "list"},
			},
		},
	},
}

func newBundleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bundle <path>",
		Short: "Create a bundle containing information about this BeeGFS instance useful for troubleshooting.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("provide a single path where the bundle should be created")
			}
			tmpBundlePath := filepath.Join(args[0], fmt.Sprintf("%s-beegfs-support-bundle", time.Now().Format(time.RFC3339)))
			err := os.Mkdir(tmpBundlePath, 0755)
			if err != nil {
				return fmt.Errorf("error creating temporary directory at %s: %w", tmpBundlePath, err)
			}
			defer os.RemoveAll(tmpBundlePath)
			err = collectSupportFiles(tmpBundlePath, getGlobalConfig())
			if err != nil {
				return fmt.Errorf("error collecting data: %w", err)
			}
			err = bundleSupportFiles(tmpBundlePath)
			if err != nil {
				return fmt.Errorf("error bundling data into a tarball at %s: %w", tmpBundlePath, err)
			}
			fmt.Printf("Successfully created support bundle at: %s%s\n\n", tmpBundlePath, bundleExtension)
			return nil
		},
	}
	// IMPORTANT: If any flags are added to this command they must be ignored in getGlobalConfig()
	// or they will be passed on to the support commands, at best causing the commands to fail, and
	// at worst causing unexpected behavior if the command has the same flag.
	return cmd
}

// getGlobalConfig allows global configuration to be automatically propagated to support commands.
// Some global configuration is skipped, such as columns, that needs to be set on individual
// commands in the bundle. It also automatically sets the --debug flag for all commands.
//
// IMPORTANT: When making updates ensure any future flags/configuration defined for the bundle
// command are also ignored.
func getGlobalConfig() []string {
	var globalArgs []string
	logger, _ := config.GetLogger()
	log := logger.With(zap.String("component", "getGlobalConfig"))

	for k, v := range viper.AllSettings() {
		var value string
		switch t := v.(type) {
		case []string:
			// Columns should be skipped as they need to be set for individual commands in the bundle.
			if k == config.ColumnsKey {
				if len(t) != 0 {
					log.Warn("setting 'fields' on the bundle command is ineffectual (must be set for individual commands in the bundle)")
				}
				continue
			}
			value = strings.Join(t, ",")
		default:
			if k == config.DebugKey {
				log.Debug(fmt.Sprintf("forcing --%s=true", config.DebugKey))
				value = "true"
			} else {
				value = fmt.Sprintf("%v", v)
			}
		}

		if value != "" {
			globalArgs = append(globalArgs, fmt.Sprintf("--%s=%s", k, value))
		} else {
			log.Debug("skipping empty global configuration", zap.Any("config", k))
		}
	}

	log.Debug("global configuration applied to all bundle commands", zap.Any("flags", globalArgs))
	return globalArgs
}

// collectSupportFiles requires an absolute path to a directory where the files listed in
// supportBundleContents will be created.
func collectSupportFiles(tmpBundlePath string, globalFlags []string) error {
	originalStdOut := os.Stdout
	originalStdErr := os.Stderr
	defer func() {
		// Restore original stdout/stderr:
		os.Stdout = originalStdOut
		os.Stderr = originalStdErr
	}()

	for _, file := range supportBundleContents {
		f, err := os.Create(filepath.Join(tmpBundlePath, file.name))
		if err != nil {
			return fmt.Errorf("error creating file %s: %w", filepath.Join(tmpBundlePath, file.name), err)
		}
		os.Stdout = f
		os.Stderr = f
		for _, c := range file.contents {
			fmt.Fprintf(f, "%s", sPrintHeader(c.sectionName, "#"))
			cmd := exec.Command(os.Args[0], append(globalFlags, c.command...)...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err := cmd.Run()
			if err != nil {
				fmt.Fprintf(f, "\nWARNING: Error capturing section: %s (ignoring)\n", err)
			}
		}
		f.Close()
	}
	return nil
}

// bundleSupportFile requires an absolute path to a directory that should be made into a tarball.
func bundleSupportFiles(tmpBundlePath string) error {
	tarGzFile, err := os.Create(tmpBundlePath + bundleExtension)
	if err != nil {
		return err
	}
	defer tarGzFile.Close()

	gzipWriter := gzip.NewWriter(tarGzFile)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	return filepath.Walk(tmpBundlePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error walking bundle directory: %w", err)
		}

		if info.IsDir() {
			return nil
		}
		return addFileToTar(tarWriter, tmpBundlePath, path)
	})
}

// addFileToTar requires an absolute tmpBundlePath to the directory containing files to be added to
// a tarball, and an absolute filePath to the specific file in this directory that should be added
// to the tarball.
func addFileToTar(tarWriter *tar.Writer, tmpBundlePath string, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file %s: %w", filePath, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat error for %s: %w", filePath, err)
	}
	header := &tar.Header{
		Name: strings.TrimPrefix(filePath, tmpBundlePath+"/"),
		Size: stat.Size(),
		Mode: int64(stat.Mode()),
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("error writing file %s header to tarball: %w", filePath, err)
	}

	_, err = io.Copy(tarWriter, file)
	if err != nil {
		return fmt.Errorf("error copying file %s contents to tarball: %s", filePath, err)
	}
	return nil
}
