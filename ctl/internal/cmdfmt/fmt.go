package cmdfmt

import (
	"fmt"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

// Printer is a condensed version of table.Writer that allows implementing alternative ways besides
// tables to write out structured data.
type Printer interface {
	AppendRow(row table.Row, configs ...table.RowConfig)
	Render() string
	SetColumnConfigs(configs []table.ColumnConfig)
}

// Printomatic provides a standard way for printing structured data.
type Printomatic struct {
	printer   Printer
	columns   []string
	printCols []string
	pageSize  uint
	rowCount  uint
	config    PrinterOptions
}

type PrinterOptions struct {
	WithEmptyColumns bool
}

type PrinterOption func(*PrinterOptions)

func WithEmptyColumns(withEmpty bool) PrinterOption {
	return func(args *PrinterOptions) {
		args.WithEmptyColumns = withEmpty
	}
}

// Creates a new Printer for printing structured data either in a tabular or JSON format. Reads
// certain Viper configuration keys to control how output is printed:
//
//   - PageSizeKey: Configure how many entries are buffered then printed at once.
//   - ColumnsKey: Define what columns/keys are printed.
//   - SortBy: Sort results by a specific column.
//
// The available columns (or keys for JSON) must be provided using the columns parameter, and will
// be used to generate the table header. When adding rows call Printomatic.AppendRow() providing as
// many columns as were initially provided. Use defaultColumns to define which columns are printed
// by default. Column names should always be lowercase. As a special case, if a user specifies
// "ColumnsKey=all" or --debug then all columns are printed.
func NewPrintomatic(columns []string, defaultColumns []string, opts ...PrinterOption) Printomatic {
	cfg := PrinterOptions{
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

	p := Printomatic{
		columns:   columns,
		printCols: printCols,
		pageSize:  viper.GetUint(config.PageSizeKey),
		config:    cfg,
	}

	p.replacePrinter()

	return p
}

// replacePrinter() refreshes the internal Printer to prepare printing a new page of output. It is
// used both to initialize the Printomatic and whenever pageSize is exceeded.
func (p *Printomatic) replacePrinter() {

	if viper.IsSet(config.PrintJsonKey) || viper.GetBool(config.PrintJsonPrettyKey) {
		p.printer = newJSONPrinter(viper.GetBool(config.PrintJsonPrettyKey))
	} else {
		// Otherwise print using a stylized table. Use a very simple style with only spaces as
		// separators to make parsing easier.
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

		if !p.config.WithEmptyColumns {
			tbl.SuppressEmptyColumns()
		}

		// Don't print a header if the page size is zero:
		if p.pageSize > 0 {
			// Build the header
			row := table.Row{}
			for _, h := range p.columns {
				row = append(row, strings.ToLower(h))
			}
			tbl.AppendHeader(row)
		}
		p.printer = tbl
	}

	// Regardless the printer type, users can control the columns:
	colCfg := []table.ColumnConfig{}

	// Hide all columns not to be printed
	for i, name := range p.columns {
		// The column number is used here because Name does not work if there is no header (as is
		// the case when pageSize=0). The ColumnConfig also recommends using this instead of name.
		// The name is still included here because it is required when printing JSON.
		colCfg = append(colCfg, table.ColumnConfig{Number: i + 1, Hidden: true, Name: name})
		for _, cName := range p.printCols {
			if cName == name || cName == "all" {
				colCfg[len(colCfg)-1].Hidden = false
				break
			}
		}
	}

	p.printer.SetColumnConfigs(colCfg)
}

// Add an item to the Printomatic. Auto prints output when pageSize rows have been added. If the
// pageSize is zero the output is always immediately printed.
func (p *Printomatic) AddItem(fields ...any) {
	p.printer.AppendRow(fields)
	p.rowCount += 1
	if p.pageSize == 0 {
		fmt.Println(p.printer.Render())
		// Intentionally don't print a blank line between rows when the page size is zero.
		p.replacePrinter()
	} else if p.rowCount%p.pageSize == 0 {
		fmt.Println(p.printer.Render())
		fmt.Println()
		p.replacePrinter()
	}
}

// Print all remaining items in the Printomatic even if pageSize has not yet been reached. This
// should always be called after adding all items to ensure everything is printed.
func (w *Printomatic) PrintRemaining() {
	// If the page size is zero rows are printed as they were added so there are no remaining rows.
	if w.pageSize != 0 && w.rowCount%w.pageSize != 0 {
		fmt.Println(w.printer.Render())
		fmt.Println()
		w.replacePrinter()
	}
}
