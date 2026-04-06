package index

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// LocalExecutor runs gufi_query as a subprocess, piping stdout into the
// results channel. It satisfies the Executor interface.
type LocalExecutor struct {
	QueryBin string // path to gufi_query binary
}

func (e *LocalExecutor) Execute(ctx context.Context, spec QuerySpec) (<-chan []string, func() error, error) {
	log, _ := config.GetLogger()

	args := buildQueryArgs(spec)
	cmd := exec.CommandContext(ctx, e.QueryBin, args...)

	log.Debug("running gufi_query",
		zap.String("bin", e.QueryBin),
		zap.Strings("args", args),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("creating stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("starting gufi_query: %w", err)
	}

	numWorkers := max(viper.GetInt(config.NumWorkersKey)-1, 1)
	rows := make(chan []string, numWorkers*4)

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(rows)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			rows <- strings.Split(scanner.Text(), spec.Delimiter)
		}
		if err := scanner.Err(); err != nil {
			cmd.Wait() //nolint:errcheck
			return fmt.Errorf("scanning gufi_query output: %w", err)
		}
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("gufi_query: %w", err)
		}
		return nil
	})

	return rows, g.Wait, nil
}

// buildQueryArgs maps non-zero QuerySpec fields to gufi_query CLI flags.
// The IndexRoot is always appended last as a positional argument.
func buildQueryArgs(spec QuerySpec) []string {
	var args []string
	addStr := func(flag, val string) {
		if val != "" {
			args = append(args, flag, val)
		}
	}
	addStr("-E", spec.SQLEntries)
	addStr("-S", spec.SQLSummary)
	addStr("-T", spec.SQLTreeSum)
	addStr("-I", spec.SQLInit)
	addStr("-K", spec.SQLAggInit)
	addStr("-J", spec.SQLIntermed)
	addStr("-G", spec.SQLAggregate)
	addStr("-F", spec.SQLFinal)
	if spec.Threads > 0 {
		args = append(args, "-n", fmt.Sprint(spec.Threads))
	}
	if spec.MinLevel > 0 {
		args = append(args, "--min-level", fmt.Sprint(spec.MinLevel))
	}
	if spec.MaxLevel >= 0 {
		args = append(args, "--max-level", fmt.Sprint(spec.MaxLevel))
	}
	if spec.PluginPath != "" {
		args = append(args, "--plugin", spec.PluginPath)
	}
	if spec.Delimiter != "" {
		args = append(args, "-d", spec.Delimiter)
	}
	args = append(args, spec.IndexRoot)
	return args
}
