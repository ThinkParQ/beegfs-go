package cmdfmt

import (
	"encoding/json"
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
)

type jsonPrinter struct {
	columns []table.ColumnConfig
	rows    []map[string]any
	pretty  bool
}

func newJSONPrinter(pretty bool) *jsonPrinter {
	return &jsonPrinter{
		rows:   []map[string]any{},
		pretty: pretty,
	}
}

func (p *jsonPrinter) SetColumnConfigs(configs []table.ColumnConfig) {
	p.columns = configs
}

func (p *jsonPrinter) AppendRow(row table.Row, configs ...table.RowConfig) {

	if len(p.columns) != len(row) {
		panic(fmt.Sprintf("unable to print json, the number of keys %d does not match the number of values %d (this is likely a bug)", len(p.columns), len(row)))
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
	if p.pretty {
		return printPrettyJSON(p.rows)
	} else {
		return printJSON(p.rows)
	}
}

func printPrettyJSON(data []map[string]any) string {
	json, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		panic("unable to marshal json (this is likely a bug): " + err.Error())
	}
	return string(json)
}

func printJSON(data []map[string]any) string {
	json, err := json.Marshal(data)
	if err != nil {
		panic("unable to marshal json (this is likely a bug): " + err.Error())
	}
	return string(json)
}
