# beegfs-go

The purpose of this repository is twofold:

* Provide common Go packages for interacting with BeeGFS.
* Host BeeGFS-related software written in Go, such as the `beegfs` command-line tool.

Repository structure:

* `common/`: Contains shared Go packages that can be imported by other projects. These packages
  provide common "low-level" functionality for interacting with BeeGFS and along with basic
  application components (logging, configuration, databases, etc.) and serve as building blocks
  "higher-level" applications and libraries.
* Top-level directories (e.g., `ctl/`): Contain "higher-level" applications and libraries
  built on the low-level functionality and other components in `common/`. Most developers interested
  in integrating BeeGFS with some external application will want to start here.

IMPORTANT: This repository is not be used for storing protocol buffers as those are not specific to
projects in Go. See the [protocol buffers](https://github.com/thinkparq/protobuf) repository for
`.proto` files and precompiled code libraries for Go and other languages.

# Using beegfs-go

The beegfs-go project is setup as a [Go module](https://go.dev/blog/using-go-modules) which is a
collection of Go packages stored in a file tree with a `go.mod` file at its root. If you're not
familiar with Go, check out the ThinkParQ [Getting Started with
Go](https://github.com/ThinkParQ/developer-handbook/tree/main/getting_started/go) section of the
developer handbook.

## Using Shared Packages

If you just want to use some common functionality in your project, first run `go get
github.com/thinkparq/beegfs-go` then import/use the shared package(s) as needed throughout your
project. The beegfs-go project is meant to be used as a Go module meaning you can (and should) pin
your `go.mod` file to a particular stable version of beegfs-go.

Individual modules can then be imported, for example to use the logging package:

```go
import "github.com/thinkparq/beegfs-go/common/logging"

myConfig := logging.Config{
    Type: "stdout",
    Level: 5,
}

func main() {
  logger, _ := logging.New(myConfig) // Ignore error for brevity.
  logger.Info("This is an informational message")
}
```
For details on using a particular package refer to its documentation. Generally, packages are
documented using Go doc comments, which can be read directly from the source files or with the
command line `go doc` (e.g., `go doc logging`). An interactive doc site can also be started using
the godoc tool (`go get golang.org/x/tools/cmd/godoc`) with `godoc -http=:8080`. Some packages may
also provide additional documentation in markdown format.

## Working with Executables

Besides common packages, this project hosts a number of components that are meant to be built into
binaries. These components generally adhere to the unofficial [Standard Go Project
Layout](https://github.com/golang-standards/project-layout). Once you have [installed
Go](https://go.dev/doc/install) there are a few options to run these components:

* Directly build and run (best for debugging): `go run ctl/cmd/beegfs/main.go`

* Install to your `$GOBIN` (best if you just want to run the applications): `go install ./ctl/cmd/beegfs/`
  * For convenience, you can also use `make install` / `make uninstall` which manages installs at `$HOME/go/bin`. 

* Build and install using OS packages: `make package-all` 
  * Install the resulting packages using `dpkg -i <package>` or similar.

Refer to the documentation included with each component for more details.

# Contributing to beegfs-go

## Coding Standards

This project strives to adhere to existing standards and best practices generally accepted by the Go
community. These include: 

* [Effective Go](https://golang.org/doc/effective_go)
* [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)

It is expected before submitting a pull request that `gofmt`. `golint`, and `go vet` have already
been run to ensure some of the more common "nitpicks" have already been addressed. You could also
run `make test` to locally execute the checks that will run in GitHub Actions.

## Documentation 

Ensure to provide quality documentation using [Go Doc comments](https://tip.golang.org/doc/comment)
for all new/updated code. Note it is generally preferred to use Go doc comments instead of providing
extensive documentation using a README.  If necessary a README can be provided, but these should
generally be limited to providing step-by-step instructions or examples for a particular use case to
help users understand generally how to use the package. Don't just reproduce API documentation in a
README as this is the intent of the Go doc comments which are used to automatically generate API
documentation. 

## Testing

Include appropriate tests for new or modified functionality and ensure that all tests pass before
submitting a pull request. Tests also often serve as [runnable
examples](https://github.com/golang/go/wiki/CodeReviewComments#examples)/

Note integration tests with external dependencies such as a mounted BeeGFS file system should use
[build constraints](https://pkg.go.dev/go/build#hdr-Build_Constraints) (also known as a build tag)
so they don't run by default. Build constraints that are in use: 

| Constraint | Requires                           |
| ---------- | ---------------------------------- |
| beegfs     | BeeGFS must be mounted /mnt/beegfs |

To specify a constraint use `-tags=<constraint>`, for example: `go test
github.com/thinkparq/beegfs-go/common/ioctl -tags=integration`
