package main

import (
	"os"

	"github.com/thinkparq/beegfs-go/beegfs-ctl/cmd/beegfs-ctl/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
