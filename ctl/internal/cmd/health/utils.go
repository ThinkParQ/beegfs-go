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

func sPrintHeader(text string, char string) string {
	repeat := strings.Repeat(char, len(text))
	repeat = repeat[:len(text)]
	return fmt.Sprintf("%s\n%s\n%s\n", repeat, text, repeat)
}

// printClientHeader prints out a standard header before displaying content for various clients.
func printClientHeader(client procfs.Client, char string) {
	mgmtd, ok := client.Config[sysMgmtdHostKey]
	if !ok {
		mgmtd = "unknown"
	}
	printHeader(fmt.Sprintf("Client ID: %s (beegfs://%s -> %s)", client.ID, mgmtd, client.Mount.Path), char)
}
