package cmdfmt

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/jedib0t/go-pretty/v6/table"
)

type jsonPrinter struct {
	columns  []table.ColumnConfig
	rows     []map[string]any
	pretty   bool
	pageSize int
}

// newJSONPrinter returns a ready to use printer. Set pretty to pretty print JSON and set pageSize
// to 0 to print NDJSON.
func newJSONPrinter(pretty bool, pageSize int) *jsonPrinter {
	return &jsonPrinter{
		// There should always be at least one element to render. This is a slight optimization when
		// rendering NDJSON.
		rows:     make([]map[string]any, 0, 1),
		pretty:   pretty,
		pageSize: pageSize,
	}
}

func (p *jsonPrinter) SetColumnConfigs(configs []table.ColumnConfig) {
	p.columns = configs
}

func (p *jsonPrinter) AppendRow(row table.Row, configs ...table.RowConfig) {

	if len(p.columns) != len(row) {
		panic(fmt.Sprintf("unable to print json, the number of columns %d does not match the number of values %d (this is likely a bug)", len(p.columns), len(row)))
	}

	item := make(map[string]any, 0)
	for i, col := range p.columns {
		if col.Hidden {
			continue
		}
		item[col.Name] = row[i]
	}
	p.rows = append(p.rows, item)
}

func (p *jsonPrinter) Render() string {

	var data any
	if p.pageSize == 0 {
		if len(p.rows) != 1 {
			panic("data contains " + strconv.Itoa(len(p.rows)) + " rows but only one row can be printed at a time with ndjson (this is likely a bug)")
		}
		data = p.rows[0]
	} else {
		data = p.rows
	}

	if p.pretty {
		return printPrettyJSON(data)
	} else {
		return printJSON(data)
	}
}

func printPrettyJSON(data any) string {
	json, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		panic("unable to marshal pretty json (this is likely a bug): " + err.Error())
	}
	return string(json)
}

func printJSON(data any) string {
	json, err := json.Marshal(data)
	if err != nil {
		panic("unable to marshal json (this is likely a bug): " + err.Error())
	}
	return string(json)
}
