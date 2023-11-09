package cmdfmt

import (
	"io"
	"text/tabwriter"
)

func NewTableWriter(output io.Writer) tabwriter.Writer {
	tw := tabwriter.Writer{}
	tw.Init(output, 0, 0, 2, ' ', 0)
	return tw
}
