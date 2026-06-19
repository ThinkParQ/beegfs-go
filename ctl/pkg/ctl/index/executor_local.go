package index

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"

	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const scannerMaxBuf = 16 << 20
const rowChanBufFactor = 4
const maxColLen = 16 << 20

// LocalExecutor runs gufi_query via --print-tlv (length-prefixed format)
// to avoid TLV delimiter collision with filenames containing `|`.
type LocalExecutor struct {
	QueryBin string
}

func (e *LocalExecutor) Execute(ctx context.Context, spec QuerySpec) (<-chan []string, func() error, error) {
	log, _ := config.GetLogger()
	ctx, cancel := context.WithCancel(ctx)

	// SECURITY (load-bearing): this relies on gufi_query being built read-only
	// (beegfs-index is packaged with ALLOW_DB_WRITES=OFF, so the --read-write
	// flag is not even compiled in and every index DB is opened read-only). That
	// is what makes it safe to run arbitrary `index query` SQL here. Args are
	// passed as an argv slice (no shell), so there is no local shell-injection
	// surface; the only injection risk is in the SQL itself, bounded by the
	// read-only build. Do not assume the Go-side escaping alone is sufficient.
	args := buildQueryArgs(spec)
	cmd := exec.CommandContext(ctx, e.QueryBin, args...)
	cmd.SysProcAttr = CallerSysProcAttr()

	log.Debug("running gufi_query",
		zap.String("bin", e.QueryBin),
		zap.String("indexRoot", spec.IndexRoot),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("creating stdout pipe: %w", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, nil, fmt.Errorf("starting gufi_query: %w", err)
	}

	rows := make(chan []string, chanBufSize(spec.Threads))

	// A plain errgroup suffices: the goroutine selects on ctx (cancelled by
	// wait), and nothing observes a derived context, so WithContext would add
	// only an unused cancel.
	var g errgroup.Group
	g.Go(func() error {
		defer close(rows)
		reader := bufio.NewReader(stdout)
		if err := parseTLV(ctx, reader, rows); err != nil {
			_ = cmd.Process.Kill()
			return errors.Join(
				fmt.Errorf("parsing gufi_query TLV: %w", err),
				cmd.Wait(),
				stderrErr(&stderr),
			)
		}
		if err := cmd.Wait(); err != nil {
			return errors.Join(
				fmt.Errorf("gufi_query: %w", err),
				stderrErr(&stderr),
			)
		}
		return nil
	})

	wait := func() error {
		cancel()
		return g.Wait()
	}
	return rows, wait, nil
}

func stderrErr(buf *bytes.Buffer) error {
	s := bytes.TrimSpace(buf.Bytes())
	if len(s) == 0 {
		return nil
	}
	return fmt.Errorf("stderr: %s", s)
}

// parseTLV consumes gufi_query --print-tlv output (ASCII-hex lengths, self-delimiting).
// Wire format: [8 hex row_len][4 hex col_count][per_col: type|lol|[lol hex col_len][col_len bytes value]].
// EOF at row boundary is clean; mid-row truncation is an error.
func parseTLV(ctx context.Context, r io.Reader, rows chan<- []string) error {
	// 8 hex chars row_len + 4 hex chars count (TLV_ROW_LEN_LEN + TLV_COL_COUNT_LEN).
	const rowPrefixLen = 12

	prefix := make([]byte, rowPrefixLen)
	hdr := make([]byte, 2)
	var excess [1]byte
	for {
		if _, err := io.ReadFull(r, prefix); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("reading row prefix: %w", err)
		}

		rowLen, err := strconv.ParseUint(string(prefix[:8]), 16, 64)
		if err != nil {
			return fmt.Errorf("parsing row_len %q: %w", prefix[:8], err)
		}
		if rowLen < rowPrefixLen {
			return fmt.Errorf("row_len %d smaller than its own %d-byte prefix", rowLen, rowPrefixLen)
		}
		count, err := strconv.ParseUint(string(prefix[8:12]), 16, 32)
		if err != nil {
			return fmt.Errorf("parsing column count %q: %w", prefix[8:12], err)
		}

		// row_len includes the prefix; bound all column reads by the remainder
		// so a header that disagrees with the serialized bytes fails on this
		// row instead of silently mis-framing every row after it.
		body := io.LimitReader(r, int64(rowLen-rowPrefixLen))

		cols := make([]string, count)
		for i := uint64(0); i < count; i++ {
			if _, err := io.ReadFull(body, hdr); err != nil {
				return fmt.Errorf("reading col %d header: %w", i, err)
			}
			lenOfLen := int(hdr[1]) - '0'
			if lenOfLen < 1 || lenOfLen > 16 {
				return fmt.Errorf("col %d: invalid length-of-length %d", i, lenOfLen)
			}

			lenBuf := make([]byte, lenOfLen)
			if _, err := io.ReadFull(body, lenBuf); err != nil {
				return fmt.Errorf("reading col %d length: %w", i, err)
			}
			colLen, err := strconv.ParseUint(string(lenBuf), 16, 64)
			if err != nil {
				return fmt.Errorf("parsing col %d length %q: %w", i, lenBuf, err)
			}
			if colLen > maxColLen {
				return fmt.Errorf("col %d len %d exceeds bound %d", i, colLen, maxColLen)
			}
			if colLen == 0 {
				continue
			}
			buf := make([]byte, colLen)
			if _, err := io.ReadFull(body, buf); err != nil {
				return fmt.Errorf("reading col %d data: %w", i, err)
			}
			cols[i] = string(buf)
		}

		if n, _ := body.Read(excess[:]); n != 0 {
			return fmt.Errorf("row_len %d exceeds the bytes consumed by its %d columns", rowLen, count)
		}

		select {
		case rows <- cols:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func buildQueryArgs(spec QuerySpec) []string {
	args := []string{"--print-tlv"}
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
	args = appendThreads(args, spec.Threads)
	if spec.MinLevel > 0 {
		args = append(args, "--min-level", fmt.Sprint(spec.MinLevel))
	}
	if spec.MaxLevel >= 0 {
		args = append(args, "--max-level", fmt.Sprint(spec.MaxLevel))
	}
	if spec.PluginPath != "" {
		args = append(args, "--plugin", spec.PluginPath)
	}
	if spec.SkipFile != "" {
		args = append(args, "--skip", spec.SkipFile)
	}
	if spec.AddUp >= 1 && spec.AddUp <= 2 {
		args = append(args, "-a", fmt.Sprint(spec.AddUp))
	}
	args = append(args, spec.IndexRoot)
	return args
}
