BeeGFS Control Tools and Libraries
==================================

`ctl` provides functionality for managing BeeGFS. It contains various "frontends" such as the
`beegfs` command-line tool that use shared "backend" functionality. The backend packages are also
setup to allow external use, and are likely what most developers interested in integrating their
applications with BeeGFS will want to leverage.

The codebase roughly adheres to the unofficial [standard Go project
layout](https://github.com/golang-standards/project-layout). Main applications intended to be built
into binaries can be found under `ctl/cmd/` and library code safe for use by external
applications can be found under `ctl/pkg/`. Internal functionality not suitable for external
reuse is under `ctl/internal`.


# Using `ctl` as a library

How to import and use CTL backend functionality from other applications can be best explained
with an example:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
)

func main() {
	// First initialize the library's global configuration store (Viper):
	config.InitViperFromExternal(
		config.GlobalConfig{
			Mount:                       "/mnt/beegfs",
			MgmtdAddress:                "auto",
			MgmtdTLSCertFile:            "/etc/beegfs/cert.pem",
			MgmtdTLSDisableVerification: false,
			MgmtdTLSDisable:             false,
			AuthFile:                    "/etc/beegfs/conn.auth",
			AuthDisable:                 false,
			LogLevel:                    3,
			NumWorkers:                  runtime.GOMAXPROCS(0),
			ConnTimeoutMs:               500,
		},
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Then calls can be made to library functions as needed:

	// Often functionality requires various internal mappings. As collecting all mappings is relatively
	// expensive, mappings should be reused when possible (until the system state/config changes):
	mappings, err := util.GetMappings(ctx)
	if err != nil && !errors.Is(err, util.ErrMappingRSTs) {
		log.Fatal(err)
	}

	getAndPrintEntry := func() {
		// To collect BeeGFS specific info about an entry:
		entryInfo, err := entry.GetEntry(ctx, mappings, entry.GetEntriesCfg{
			Verbose:        false,
			IncludeOrigMsg: false,
		}, "/helloworld") // This function requires a relative path inside BeeGFS.

		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Entry Info - Path: %s, Entry ID: %s", entryInfo.Entry.FileName, entryInfo.Entry.EntryID)
		if entryInfo.Entry.Details != nil {
			fmt.Printf(", File State: %s", entryInfo.Entry.Details.FileState.String())
		}
		fmt.Println()
	}
	getAndPrintEntry()

	// Data management applications such as HSM solutions may wish to set access flags to
	// temporarily block regular client access:
	if err = entry.SetAccessFlags(ctx, mappings, "/helloworld", beegfs.AccessFlagWriteLock); err != nil {
		log.Fatal(err)
	}
	getAndPrintEntry()
	if err = entry.SetAccessFlags(ctx, mappings, "/helloworld", beegfs.AccessFlagUnlocked); err != nil {
		log.Fatal(err)
	}
	getAndPrintEntry()
}

```