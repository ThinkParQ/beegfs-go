package main

import (
	"os"

	"github.com/thinkparq/beegfs-go/beegfs-ctl/internal/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
