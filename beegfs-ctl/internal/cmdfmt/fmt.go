package cmdfmt

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/thinkparq/gobee/beegfs"
)

func NewTableWriter(output io.Writer) tabwriter.Writer {
	tw := tabwriter.Writer{}
	tw.Init(output, 0, 0, 2, ' ', 0)
	return tw
}

func PrintNodeInfoHeader(w *tabwriter.Writer, includeUid bool) {
	if includeUid {
		fmt.Fprintf(w, "UID\t")
	}
	fmt.Fprintf(w, "NodeID\tAlias\t")
}

func PrintNodeInfoRow(w *tabwriter.Writer, n beegfs.Node, includeUid bool) {
	if includeUid {
		fmt.Fprintf(w, "%s\t", n.Uid)
	}
	fmt.Fprintf(w, "%s\t%s\t", n.Id, n.Alias)
}
