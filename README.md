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


## Versioning

BeeGFS OS packages/binaries built from this repository and the `beegfs-go` module that provides
reusable Go packages currently follow slightly different versioning schemes:

* OS packages/binaries built from this repository follow the same versioning scheme as the other BeeGFS
  components and start at version `v8.x.y`. 
  * For example if you want to build the version of the `beegfs` tool compatible with BeeGFS 8.0.0
    you would check out the `v8.0.0` tag.
* The Go module and reusable packages it provides will remain at `v0` indicating the Go module's API
  is not necessarily guaranteed to be stable and each `v8.x.y` BeeGFS release, the module's API will
  be versioned as `v0.x.y`.
  * For example, to import Go packages from the `beegfs-go` module that work with BeeGFS
    `v8.0.0-beta.0` you would run `go get github.com/thinkparq/beegfs-go@v0.0.0-beta.0`.
  * If you make changes to `beegfs-go` that need to be imported elsewhere before a new "official"
    version is tagged, you would use a [pseudo-version](https://go.dev/ref/mod#pseudo-versions) by
    running `go get github.com/thinkparq/beegfs-go@<LONG-COMMIT-HASH>` to import a specific commit.

### FAQs:

#### Why not just use the same version for the Go module and binaries it provides?

There are a few reasons for this:

First, Go has specific rules around [module version
numbering](https://go.dev/doc/modules/version-numbers). Once we move past `v0` or `v1`, the module
path must be updated to include the version (i.e., `github.com/thinkparq/beegfs-go/v8`). However, it
would be ideal to provide a single Go module that maintains compatibility across different BeeGFS
versions. This approach simplifies development for maintainers of `beegfs-go` (as there is no need
to backport fixes to multiple branches) and makes it easier for external users. Since we cannot yet
determine if future major BeeGFS versions will require breaking changes to the Go module, there is
no immediate need to synchronize the versions.

Second, moving past `v0` signals that the module's external API is stable and guaranteed not to
change. While we believe the external API is largely stable, we think it is still prudent to wait
before making this guarantee to users.

In short, moving past v0 is a one way trip. Until there is a compelling reason to do so, sticking at
`v0` leaves more options available for how to handle module versioning in the future.

#### Is there precedence for this?

Yes, Kubernetes releases are versioned as `v1.31.0`, but the [`kubernetes/client-go`
module](https://github.com/kubernetes/client-go?tab=readme-ov-file#versioning) is versioned as
`v0.31.0`, signaling no API stability is guaranteed (and they do break their API from time to time).

#### Aren't we worried eventually the version namespaces will collide? 

If/when we bump from `v0` we would likely either bump to `v1` if we think we can provide one module
that works across multiple major BeeGFS versions, or bump directly to sync with the BeeGFS major
version if backward compatibility cannot be ensured.

#### Why not just use different tags for the module and binaries?

We use GoReleaser to build binaries and OS packages. It expects to work with a tag that follows
semantic versioning rules, and currently does not appear to provide a way to extract a semver from a
non-semver tag. However we only run GoReleaser on tags prefixed with `v8.` and the actions workflow
is configured to only run on the pushed tag and determine the previous tag based on the most recent
semantic version before the current tag.

## Working with Executables

Besides providing shared Go packages, this project hosts a number of components that are meant to be
built into binaries. These components generally adhere to the unofficial [Standard Go Project
Layout](https://github.com/golang-standards/project-layout). Once you have [installed
Go](https://go.dev/doc/install) there are a few options to run these components:

* Directly build and run (best for debugging): `go run ctl/cmd/beegfs/main.go`

* Install to your `$GOBIN` (best if you just want to run the applications): `go install ./ctl/cmd/beegfs/`
  * For convenience, you can also use `make install` / `make uninstall` which manages installs at `$HOME/go/bin`. 

* Build and install using OS packages: `make package-all` 
  * Install the resulting packages using `dpkg -i <package>` or similar.

Refer to the documentation included with each component for more details.

## Importing functionality into other Go projects

If you just want to use some common functionality in your project, first run `go get
github.com/thinkparq/beegfs-go@latest` then import/use the shared package(s) as needed throughout
your project. The `beegfs-go` project is meant to be used as a Go module meaning you can (and
should) pin your `go.mod` file to a particular stable version of beegfs-go. See the versioning
section above to ensure you use the correct version.

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
