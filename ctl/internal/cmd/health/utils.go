package health

import (
	"fmt"
	"strings"

	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
)

const (
	sysMgmtdHostKey = "sysMgmtdHost"
)

func printHeader(text string, char string) {
	fmt.Printf("%s", sPrintHeader(text, char))
}

// sPrintHeader prints the text with repeating char above and below. Newlines are respected and the
// width of char is based on the longest line with shorter lines centered.
func sPrintHeader(text string, char string) string {
	lines := strings.Split(text, "\n")
	longestLine := 0
	longestIdx := 0
	for i, line := range lines {
		if len(line) > longestLine {
			longestLine = len(line)
			longestIdx = i
		}
	}
	repeat := strings.Repeat(char, longestLine)
	repeat = repeat[:longestLine]

	var str = strings.Builder{}
	str.WriteString(repeat + "\n")
	for i, line := range lines {
		if i == longestIdx {
			str.WriteString(line + "\n")
		} else {
			pad := (longestLine - len(line)) / 2
			str.WriteString(strings.Repeat(" ", pad) + line + "\n")
		}
	}
	str.WriteString(repeat + "\n")
	return str.String()
}

// printClientHeader prints out a standard header before displaying content for various clients.
func printClientHeader(client procfs.Client, char string) {
	mgmtd, ok := client.Config[sysMgmtdHostKey]
	if !ok {
		mgmtd = "unknown"
	}
	printHeader(fmt.Sprintf("Client ID: %s (beegfs://%s -> %s)", client.ID, mgmtd, client.Mount.Path), char)
}
