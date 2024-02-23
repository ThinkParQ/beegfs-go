package main

import (
	"os"

	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
