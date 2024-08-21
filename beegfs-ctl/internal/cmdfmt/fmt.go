package cmdfmt

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/gobee/beegfs"
)

func NewDeprecatedTableWriter(output io.Writer) tabwriter.Writer {
	tw := tabwriter.Writer{}
	tw.Init(output, 0, 0, 2, ' ', 0)
	return tw
}

func PrintNodeInfoHeaderDeprecated(w *tabwriter.Writer, includeUid bool) {
	if includeUid {
		fmt.Fprintf(w, "UID\t")
	}
	fmt.Fprintf(w, "NodeID\tAlias\t")
}

func PrintNodeInfoRowDeprecated(w *tabwriter.Writer, n beegfs.Node, includeUid bool) {
	if includeUid {
		fmt.Fprintf(w, "%s\t", n.Uid)
	}
	fmt.Fprintf(w, "%s\t%s\t", n.Id, n.Alias)
}

type TableWrapper struct {
	tableWriter table.Writer
	columns     []string
	printCols   []string
	pageSize    uint
	rowCount    uint
	config      TableWrapperOptions
}

type TableWrapperOptions struct {
	WithEmptyColumns bool
}

type TableWrapperOption func(*TableWrapperOptions)

func WithEmptyColumns(withEmpty bool) TableWrapperOption {
	return func(args *TableWrapperOptions) {
		args.WithEmptyColumns = withEmpty
	}
}

// Creates a new table.Writer, applying some default settings. Reads certain viper config keys to
// configure the table: PageSizeKey to configure the page size, ColumnsKey to define the columns to
// be printed and SortBy key to sort the results by a column. The columns of the table must be
// provided using the columns parameter (the header is generated from them). When adding rows,
// call table.AppendRow() providing as many columns as set for the header. defaultColumns defines
// which columns to print by default. A user can do --columns=all to print all columns instead.
// Column names should be lowercase.
func NewTableWrapper(columns []string, defaultColumns []string, opts ...TableWrapperOption) TableWrapper {
	cfg := TableWrapperOptions{
		WithEmptyColumns: true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Determine the columns to be printed
	printCols := defaultColumns
	if viper.IsSet(config.ColumnsKey) {
		printCols = viper.GetStringSlice(config.ColumnsKey)
	}

	w := TableWrapper{
		columns:   columns,
		printCols: printCols,
		pageSize:  viper.GetUint(config.PageSizeKey),
		config:    cfg,
	}

	// If pageSize is > 0, we want to use TableWriter
	if w.pageSize > 0 {
		w.replaceTableWriter()
	}

	return w
}

// Creates a new internal TableWriter to prepare printing a new page
func (w *TableWrapper) replaceTableWriter() {
	if w.pageSize == 0 {
		return
	}

	// Set the style. We use a very simple style with only spaces as separators to make parsing
	// easier.
	tbl := table.NewWriter()
	tbl.SetStyle(table.Style{
		Box: table.BoxStyle{
			PaddingRight:  "  ",
			PageSeparator: "\n",
		},
		Format: table.FormatOptions{
			// Direction breaks SuppressEmptyColumns() due to a bug in go-pretty where
			// initForRenderSuppressColumns() does not ignore control characters. This was fixed
			// with https://github.com/jedib0t/go-pretty/pull/327. Direction could be reenabled once
			// a new go-pretty release is tagged. But it also results in hidden characters being
			// added to columns which are included if a user copies text, which can lead to weird
			// behavior (for example copying a unix timestamp to a timestamp converter website).
			// We should leave this disabled unless it becomes necessary.
			//
			// Direction: text.LeftToRight,
			Footer: text.FormatUpper,
			Header: text.FormatUpper,
		},
	})

	if !w.config.WithEmptyColumns {
		tbl.SuppressEmptyColumns()
	}

	// Build the header
	row := table.Row{}
	for _, h := range w.columns {
		row = append(row, strings.ToLower(h))
	}
	tbl.AppendHeader(row)

	colCfg := []table.ColumnConfig{}

	// Hide all columns not to be printed
	for _, name := range w.columns {
		colCfg = append(colCfg, table.ColumnConfig{Name: name, Hidden: true})
		for _, cName := range w.printCols {
			if cName == name || cName == "all" {
				colCfg[len(colCfg)-1].Hidden = false
				break
			}
		}
	}

	tbl.SetColumnConfigs(colCfg)

	w.tableWriter = tbl
}

// Appends a Row to the TableWriter if used, prints row to stdout otherwise. Auto prints the table
// if pageSize rows have been added/
func (w *TableWrapper) Row(fields ...any) {
	if w.tableWriter != nil {
		w.tableWriter.AppendRow(fields)
		w.rowCount += 1

		if w.rowCount%w.pageSize == 0 {
			fmt.Println(w.tableWriter.Render())
			fmt.Println()
			w.replaceTableWriter()
		}
	} else {
		for i, f := range fields {
			if len(w.columns) > i {
				for _, cName := range w.printCols {
					if cName == w.columns[i] || cName == "all" {
						fmt.Printf("%v  ", f)
						break
					}
				}
			}
		}
		fmt.Println()
	}
}

// Prints the remaining rows of the table if tableWriter is used.
func (w *TableWrapper) PrintRemaining() {
	if w.tableWriter != nil && w.rowCount%w.pageSize != 0 {
		fmt.Println(w.tableWriter.Render())
		fmt.Println()
		w.replaceTableWriter()
	}
}
