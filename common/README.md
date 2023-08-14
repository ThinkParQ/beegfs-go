GoBee
=====

GoBee is a library of common packages used when implementing projects in Go at
ThinkParQ around the BeeGFS file system. Provided functionality includes
configuration management and logging, along with various data structures and
custom types.

IMPORTANT: This repository should not be used for storing protocol buffers as
those are not specific to projects in Go. See the [protocol
buffers](https://github.com/thinkparq/protobuf) repository for `.proto` files
and precompiled code libraries for Go and other languages.

# Why does this repository exist? 

The goal of GoBee is twofold:

(1) Provide a similar user experience across all Go projects in the BeeGFS
ecosystem for common functionality such as logging or configuration.

(2) Avoid duplicate implementations of the same functionality in different
projects by providing a central repository of proven functionality that can be
easily imported and updated using `go mod`. This prevents developers from
needing to copy/paste essentially identical blocks of code in multiple projects
creating a maintainability nightmare whenever fixes/enhancements are made to one
implementation.

In short, if you're developing functionality outside the core business logic of
your application, it probably already exists or should be implemented as part of
GoBee

# Getting Started

To install GoBee run `go get github.com/thinkparq/gobee`. GoBee is meant to be
used as a Go module meaning you can (and should) pin your `go.mod` file to a
particular stable version of GoBee. 

Individual modules can then be imported, for example to use the logging package:

```go
import "github.com/thinkparq/gobee/logging"

myConfig := logging.Config{
    Type: "stdout",
    Level: 5,
}

func main() {
  logger, _ := logging.New(myConfig) // Ignore error for brevity.
  logger.Info("This is an informational message")
}
```

For details on using a particular package refer to its documentation. Generally,
packages are documented using Go doc comments, which can be read directly from
the source files or with the command line `go doc` (e.g., `go doc logging`). An
interactive doc site can also be started using the godoc tool (`go get
golang.org/x/tools/cmd/godoc`) with `godoc -http=:8080`. Some packages may also
provide additional documentation in markdown format.

# Contributing to GoBee

## Project Structure 
GoBee is setup as a [module](https://go.dev/blog/using-go-modules) which is a
collection of Go packages stored in a file tree with a `go.mod` file at its
root. The packages in this module can be imported using
`github.com/thinkparq/gobee/<PACKAGE_NAME>`. If you are just adding a new type
or data structure it can generally just be added under `types/`, otherwise new
packages are generally created as top-level directories though in the future it
may make sense to group related packages under a sub-directory. 

## Coding Standards

This project generally adheres to existing standards and best practices
generally accepted by the Go community. These include: 

* [Effective Go](https://golang.org/doc/effective_go)
* [Go Code Review
  Comments](https://github.com/golang/go/wiki/CodeReviewComments)

It is expected before submitting a pull request that `gofmt`. `golint`, and `go
vet` have already been run to ensure some of the more common "nitpicks" have
already been addressed. 

## Documentation 

Ensure to provide quality documentation using [Go Doc
comments](https://tip.golang.org/doc/comment) for all new/updated code. Note it
is generally preferred to use Go doc comments instead of providing extensive
documentation using a README.  If necessary a README can be provided, but these
should generally be limited to providing step-by-step instructions or examples
for a particular use case to help users understand generally how to use the
package. Don't just reproduce API documentation in a README as this is the
intent of the Go doc comments which are used to automatically generate API
documentation. 

## Testing

Include appropriate tests for new or modified functionality and ensure that all
tests pass before submitting a pull request. Tests also often serve as
[runnable
examples](https://github.com/golang/go/wiki/CodeReviewComments#examples)/
