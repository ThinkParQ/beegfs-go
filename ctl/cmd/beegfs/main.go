package main

import (
	"os"

	"github.com/thinkparq/beegfs-go/v8/ctl/internal/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
