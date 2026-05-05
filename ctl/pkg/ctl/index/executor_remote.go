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

// RemoteExecutor runs queries via gufi_vt inside gufi_sqlite3 :memory:.
// For a single host it creates one virtual table; for N hosts it creates N
// virtual tables and UNION ALL's them — SQL composition is identical either way.
type RemoteExecutor struct {
	Hosts      []string // 1 or more remote hosts
	Sqlite3Bin string   // path to gufi_sqlite3
}

func (e *RemoteExecutor) Execute(ctx context.Context, spec QuerySpec) (<-chan []string, func() error, error) {
	log, _ := config.GetLogger()

	sql := buildVTSQL(spec, e.Hosts)
	args := []string{":memory:"}
	if spec.Delimiter != "" {
		args = []string{"-d", spec.Delimiter, ":memory:"}
	}
	cmd := exec.CommandContext(ctx, e.Sqlite3Bin, args...)
	cmd.Stdin = strings.NewReader(sql)

	log.Debug("running gufi_sqlite3 (remote VT)",
		zap.String("bin", e.Sqlite3Bin),
		zap.Strings("hosts", e.Hosts),
		zap.String("sql", sql),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("creating stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("starting gufi_sqlite3: %w", err)
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
			return fmt.Errorf("scanning gufi_sqlite3 output: %w", err)
		}
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("gufi_sqlite3: %w", err)
		}
		return nil
	})

	return rows, g.Wait, nil
}

// buildVTSQL generates a single-line SQL string for gufi_sqlite3 stdin.
// gufi_sqlite3 cannot process multi-line SQL via stdin, so all statements
// must be on one line.
//
// gufi_vt's internal set_refstr() strips ALL leading/trailing quote characters
// from parameter values, which mangles SQL ending with a single-quoted literal
// (e.g. "WHERE name NOT LIKE '.%'"). To avoid this, the top-level WHERE
// predicate is extracted from E/S SQL via vtSplitSQL and applied on the outer
// SELECT instead, where SQLite handles quoting correctly.
//
// BeeGFS subquery templates ("FROM (SELECT * FROM entries WHERE %s) AS e LEFT JOIN …")
// have the WHERE at depth 1 so vtSplitSQL leaves them unchanged; they end with
// "e.inode" (no trailing quote) and are safe to pass directly.
func buildVTSQL(spec QuerySpec, hosts []string) string {
	var sb strings.Builder

	eVT, outerPred := vtSplitSQL(spec.SQLEntries)
	sVT, sPred := vtSplitSQL(spec.SQLSummary)
	if outerPred == "" {
		outerPred = sPred
	}

	safeSpec := spec
	safeSpec.SQLEntries = eVT
	safeSpec.SQLSummary = sVT

	for i, host := range hosts {
		fmt.Fprintf(&sb, "CREATE VIRTUAL TABLE temp.q%d USING gufi_vt(", i)
		writeVTParams(&sb, safeSpec, host)
		sb.WriteString("); ")
	}

	// UNION ALL SELECT. Wrap in a subquery so the outer WHERE applies to all
	// hosts uniformly when more than one host is present.
	if len(hosts) == 1 {
		sb.WriteString("SELECT * FROM temp.q0")
	} else {
		sb.WriteString("SELECT * FROM (SELECT * FROM temp.q0")
		for i := 1; i < len(hosts); i++ {
			fmt.Fprintf(&sb, " UNION ALL SELECT * FROM temp.q%d", i)
		}
		sb.WriteString(")")
	}
	if outerPred != "" {
		sb.WriteString(" WHERE " + outerPred)
	}
	sb.WriteString(";")

	return sb.String()
}

// vtSplitSQL splits a SQL string at its filterable WHERE clause, returning:
//   - vtSQL: the SQL with the predicate replaced by "1" (safe for gufi_vt parameters)
//   - pred:  the original predicate for use in the outer SELECT
//
// Two patterns are handled:
//
//  1. Top-level WHERE (non-BeeGFS templates):
//     "SELECT … FROM entries WHERE <pred>"
//     → vtSQL = "SELECT … FROM entries WHERE 1", pred = "<pred>"
//
//  2. BeeGFS subquery pattern — WHERE is at depth 1 inside the first (…):
//     "SELECT … FROM (SELECT * FROM entries WHERE <pred>) AS e LEFT JOIN …"
//     → vtSQL replaces the inner predicate with 1, pred = "<pred>"
//     The outer SELECT then receives: SELECT * FROM temp.q WHERE <pred>
//     using only the unqualified column names exposed by the VT (name, type, …).
//
// If neither pattern is found the SQL is returned unchanged with an empty pred.
func vtSplitSQL(sql string) (vtSQL, pred string) {
	if sql == "" {
		return "", ""
	}

	const kw = " WHERE "
	depth := 0
	i := 0
	firstSubqueryWhereAt := -1 // position of " WHERE " at depth 1

	for i < len(sql) {
		ch := sql[i]
		switch ch {
		case '(':
			depth++
			i++
			continue
		case ')':
			depth--
			i++
			continue
		case '\'':
			// skip SQL string literal, respecting '' escaping
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					i++
					if i < len(sql) && sql[i] == '\'' {
						i++ // escaped quote — stay in string
						continue
					}
					break // end of string literal
				}
				i++
			}
			continue
		}

		if i+len(kw) > len(sql) {
			i++
			continue
		}
		chunk := sql[i : i+len(kw)]
		if !strings.EqualFold(chunk, kw) {
			i++
			continue
		}

		if depth == 0 {
			// Top-level WHERE: predicate runs to end of string.
			return sql[:i] + " WHERE 1", sql[i+len(kw):]
		}
		if depth == 1 && firstSubqueryWhereAt < 0 {
			firstSubqueryWhereAt = i
		}
		i++
	}

	// No top-level WHERE. Try to extract the predicate from the first
	// depth-1 subquery (BeeGFS pattern: FROM (SELECT * FROM tbl WHERE <pred>) AS alias …).
	if firstSubqueryWhereAt >= 0 {
		whereStart := firstSubqueryWhereAt + len(kw)
		// find the matching ')' that closes the subquery
		d := 0
		j := whereStart
		predEnd := -1
		for j < len(sql) {
			switch sql[j] {
			case '(':
				d++
			case ')':
				if d == 0 {
					predEnd = j
				} else {
					d--
				}
			}
			if predEnd >= 0 {
				break
			}
			j++
		}
		if predEnd > whereStart {
			innerPred := sql[whereStart:predEnd]
			vtSQL = sql[:firstSubqueryWhereAt] + " WHERE 1" + sql[predEnd:]
			return vtSQL, innerPred
		}
	}

	return sql, ""
}

func writeVTParams(sb *strings.Builder, spec QuerySpec, host string) {
	// All params are emitted as a comma-separated list on one line.
	// gufi_vt uses key=value pairs; string values are single-quoted with '' escaping.
	params := make([]string, 0, 12)
	addParam := func(key, val string) {
		if val != "" {
			params = append(params, fmt.Sprintf("%s='%s'", key, strings.ReplaceAll(val, "'", "''")))
		}
	}
	addIntParam := func(key string, val int) {
		if val > 0 {
			params = append(params, fmt.Sprintf("%s=%d", key, val))
		}
	}
	addLevelParam := func(key string, val int) {
		if val >= 0 {
			params = append(params, fmt.Sprintf("%s=%d", key, val))
		}
	}

	addParam("E", spec.SQLEntries)
	addParam("S", spec.SQLSummary)
	addParam("T", spec.SQLTreeSum)
	addParam("I", spec.SQLInit)
	addParam("K", spec.SQLAggInit)
	addParam("J", spec.SQLIntermed)
	addParam("G", spec.SQLAggregate)
	addParam("F", spec.SQLFinal)
	addIntParam("threads", spec.Threads)
	addIntParam("min_level", spec.MinLevel)
	addLevelParam("max_level", spec.MaxLevel)
	addParam("plugin", spec.PluginPath)
	params = append(params, fmt.Sprintf("index='%s'", strings.ReplaceAll(spec.IndexRoot, "'", "''")))
	params = append(params, "remote_cmd='ssh'")
	params = append(params, fmt.Sprintf("remote_arg='%s'", strings.ReplaceAll(host, "'", "''")))

	sb.WriteString(strings.Join(params, ", "))
}
