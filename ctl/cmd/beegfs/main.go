package main

import (
	"os"

	"github.com/thinkparq/beegfs-go/ctl/internal/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
