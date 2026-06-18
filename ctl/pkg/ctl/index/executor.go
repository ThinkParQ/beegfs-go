package index

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"syscall"
)

var sshHostRE = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*(@[A-Za-z0-9_][A-Za-z0-9._-]*)?$`)

func validateSSHHost(host string) error {
	// The regex also rejects an empty host and a leading '-' (ssh option
	// injection): the mandatory first character class allows neither.
	if !sshHostRE.MatchString(host) {
		return fmt.Errorf("invalid ssh host %q (expected [user@]host with A-Z a-z 0-9 . _ -, not starting with '-')", host)
	}
	return nil
}

const MaxLevelUnlimited = -1

type QuerySpec struct {
	IndexRoot     string // positional arg / index= param
	IndexRootGlob bool
	SQLEntries    string // -E: per-entry SQL (runs against each entries table)
	SQLSummary    string // -S: per-directory SQL (runs against each summary table)
	SQLTreeSum    string // -T: tree-summary SQL
	SQLInit       string // -I: create per-thread intermediate table (in each thread's output DB)
	SQLAggInit    string // -K: create global aggregate table
	SQLIntermed   string // -J: copy per-thread intermediate into global aggregate (once per thread)
	SQLAggregate  string // -G: final SELECT from global aggregate
	SQLFinal      string // -F: post-aggregate SELECT (rarely used)
	Threads       int
	MinLevel      int
	MaxLevel      int
	PluginPath    string
	SkipFile      string
	AddUp         int
	Columns       []string // declared output column names for the final SELECT stage
}

// Executor runs a GUFI query and streams result rows. The returned wait
// function must be called exactly once after the caller is done with the
// channel: it tears down the underlying query (killing the subprocess if the
// channel was not fully drained) and returns the query's final status.
// Draining the channel before calling wait yields the query result; calling
// wait early cancels the query and reports the cancellation as an error.
type Executor interface {
	Execute(ctx context.Context, spec QuerySpec) (<-chan []string, func() error, error)
}

func NewExecutor(cfg GlobalCfg) (Executor, error) {
	host, err := resolveSSHHost(cfg.IndexAddr)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return &LocalExecutor{QueryBin: QueryBin}, nil
	}
	return &RemoteExecutor{
		Host:       host,
		Sqlite3Bin: Sqlite3Bin,
	}, nil
}

// CallerSysProcAttr makes an index subprocess run with the caller's real uid/gid.
// The CLI is installed setgid beegfs (to read the group-beegfs auth secret), which
// otherwise leaves children with egid=beegfs and lets GUFI enforce index and
// source-tree permissions as the beegfs group rather than the invoking user.
func CallerSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:         uint32(os.Getuid()),
			Gid:         uint32(os.Getgid()),
			NoSetGroups: true,
		},
	}
}

// chanBufSize sizes a result/line channel buffer from the GUFI thread count.
// Threads is the producing subprocess's parallelism (passed to the gufi binaries
// as -n), so it tracks how many rows can accumulate between consumer reads. The
// floor of 1 covers Threads==0 ("let GUFI choose"): the buffer must still
// decouple the single producing subprocess from the single consuming goroutine.
func chanBufSize(threads int) int {
	return max(threads, 1) * rowChanBufFactor
}

// runSubprocess runs bin with args, streaming each line of its combined
// stdout+stderr to lines until the process exits or ctx is cancelled. Shared by
// Create and Rescan; the executors parse structured output and do not use it.
func runSubprocess(ctx context.Context, bin string, args []string, lines chan<- string) error {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.SysProcAttr = CallerSysProcAttr()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("creating stdout pipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting %s: %w", bin, err)
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64<<10), scannerMaxBuf)
	for scanner.Scan() {
		select {
		case lines <- scanner.Text():
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			waitErr := cmd.Wait()
			return errors.Join(ctx.Err(), waitErr)
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		_ = cmd.Process.Kill()
		waitErr := cmd.Wait()
		return errors.Join(fmt.Errorf("scanning %s output: %w", bin, scanErr), waitErr)
	}
	return cmd.Wait()
}
