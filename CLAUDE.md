# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the BeeGFS Go repository — a single [Go module](https://go.dev/blog/using-go-modules)
(`github.com/thinkparq/beegfs-go`) that serves two purposes:

- **Applications** — the `beegfs` CLI, the RST services (`beegfs-remote`, `beegfs-sync`), and
  `beegfs-watch`.
- **Reusable libraries** — packages under `common/` (and the `pkg/` trees) that external Go projects
  import to interact with BeeGFS.

The broader BeeGFS stack spans three repositories:
- **beegfs-go** (this repo) — CLI tooling and RST services (Go)
- **beegfs-rust** — Management service `beegfs-mgmtd` (Rust)
- **beegfs-core** or **beegfs** — C/C++ daemons and kernel module

Protocol buffer definitions are **not** stored here — they live in the sibling
`github.com/thinkparq/protobuf` repo and are imported as a module dependency (`protobuf/go/...`).
Do not copy or regenerate proto files here; when a `.proto` changes there, bump the
`github.com/thinkparq/protobuf` version in `go.mod`.

## Repository layout

Each top-level application directory follows the unofficial [Standard Go Project
Layout](https://github.com/golang-standards/project-layout):
- `cmd/` — `main.go` entrypoints, built into binaries
- `pkg/` — library code safe for external import (the "backend")
- `internal/` — private code, not importable by other modules (the "frontend" / wiring)

| Directory | Role |
|-----------|------|
| `common/` | Shared low-level packages; importable by external projects |
| `ctl/` | `beegfs` CLI binary + public control libraries |
| `rst/` | Remote Storage Targets — `beegfs-remote` coordinator and `beegfs-sync` worker |
| `watch/` | `beegfs-watch` filesystem event streaming daemon |

### Binaries

| Binary | Entry point | Purpose |
|--------|-------------|---------|
| `beegfs` | `ctl/cmd/beegfs/` | Control CLI for BeeGFS management |
| `beegfs-remote` | `rst/remote/cmd/beegfs-remote/` | RST coordinator service (gRPC) |
| `beegfs-sync` | `rst/sync/cmd/beegfs-sync/` | RST worker that moves data to/from S3 |
| `beegfs-watch` | `watch/cmd/beegfs-watch/` | Filesystem event listener / gRPC router |

## Commands

Toolchain versions are pinned in `go.mod` (currently Go 1.26.4). Dev tools (`staticcheck`,
`govulncheck`, `go-licenses`) are declared in the `go.mod` `tool` block and run via `go tool <name>`.

### Build and run
```bash
go run ./ctl/cmd/beegfs             # Build & run directly (best for debugging)
go install ./ctl/cmd/beegfs        # Install to $GOBIN
make install                       # Install all binaries to $HOME/go/bin
make install-ctl                   # Install one binary (also: -remote, -sync, -watch)
make uninstall / uninstall-<name>  # Remove
```

### Test
```bash
make test                          # Full CI suite (see note below)
make test-unit                     # Unit tests only (go test ./...)
go test ./common/beegfs            # Single package
go test -run TestFoo ./ctl/pkg/ctl/entry   # Single test
```

`make test` is exactly what CI runs (`.github/workflows/checks.yml`): `check-go-version` +
`check-gofmt` + `check-linters` + `check-go-tidy` + `test-unit` + `check-vulnerabilities` +
`check-licenses`.

**Note:** `make test` *mutates the working tree* — it runs `go mod tidy` and regenerates the
per-binary `NOTICE.md` files to verify they were committed. Run `git reset --hard` /
`git checkout` afterward if running locally.

### Lint and format
```bash
go fmt ./...                       # Format (check-gofmt enforces this)
make check-linters                 # go tool staticcheck ./... && go vet ./...
go tool govulncheck ./...          # Vulnerability scan
make tidy                          # go mod tidy
```

### Packaging and licenses
```bash
make package-all                   # GoReleaser snapshot → dist/ (.deb/.rpm); requires goreleaser
make generate-notices              # Regenerate per-binary NOTICE.md via go-licenses
make check-licenses                # Enforce license allowlist + NOTICE freshness
```

## Architecture

### `ctl/` — the `beegfs` CLI (frontend/backend split)

The CLI is deliberately layered so the logic is reusable as a library:

- `ctl/cmd/beegfs/main.go` — thin entrypoint.
- `ctl/internal/cmd/` — **Cobra command definitions** (flags, arg parsing, output). One
  subdirectory per command group (14 in total): `benchmark`, `buddygroup`, `copy`, `debug`,
  `entry`, `health`, `index`, `license`, `node`, `pool`, `quota`, `rst`, `stats`, `target`.
  `root.go` wires them together.
- `ctl/pkg/ctl/` — **backend business logic**, importable by external apps, mirroring the command
  groups above. This is where the real work happens and is the public API surface for library
  consumers.
- `ctl/pkg/config` — global configuration store (Viper). External callers initialize it with
  `config.InitViperFromExternal(...)` before invoking backend functions (see `ctl/README.md`).
- `ctl/internal/` — private helpers: `bflag` (BeeGFS-specific pflag types), `cmdfmt` (output
  formatting), `config`, `util`.

Backend packages talk to daemons over two protocols: **BeeMsg** (via `common/beemsg`, to
meta/storage) and **gRPC** (via `protobuf/go/{beegfs,management,beeremote,flex,license}` clients, to
mgmtd/remote).

### `common/` — shared foundation

Low-level packages imported across all applications (and by external projects). If functionality is
outside an app's core business logic, it probably belongs here.

| Package | Role |
|---------|------|
| `beegfs/` | Core domain types — entities, entity IDs, entries, node types, consistency states, parsers |
| `beemsg/` | BeeMsg binary wire protocol — message assembly/parsing + `nodestore` |
| `ioctl/` | BeeGFS-specific kernel client ioctls (create-file, stat, etc.) |
| `filesystem/` | Filesystem abstractions and path filtering (afero-based) |
| `rst/` | Remote Storage Target abstraction shared by remote & sync — S3 client, job requests, builders |
| `registry/` | Service discovery and capability/feature management |
| `kvstore/` | Thread-safe data structures backed by BadgerDB (`MapStore`); used by the RST services |
| `logger/` | Structured logging (zap), with OpenTelemetry (`otelzap`) and Badger log bridges |
| `configmgr/`, `telemetry/`, `scheduler/`, `types/`, `strfmt/`, `probecache/` | Config mgmt, OTel, and supporting utilities |

### `rst/` — Remote Storage Targets (HSM / data movement)

- `rst/remote/` (`beegfs-remote`) — coordinator. `internal/`: `config`, `job` (manager +
  `pendingsync`), `server` (gRPC), `worker`, `workermgr`.
- `rst/sync/` (`beegfs-sync`) — worker processes that perform the actual transfers to/from
  S3-compatible object storage (stubbing, offloading, rehydration).
- Service definitions live in `protobuf/proto/beeremote.proto` and `flex.proto`.

**Job flow:** `beegfs-remote` receives `SubmitJob` gRPC calls, persists jobs via `kvstore`, splits
them into work requests, and dispatches to `beegfs-sync` workers. A worker records progress in a
local `WorkJournal`, processes the request, then calls back to remote with `UpdateWork`.

### `watch/` — filesystem event streaming (`beegfs-watch`)

Replaces the deprecated C++ `beegfs-event-listener` in `beegfs-core`.
- `internal/metadata/` — connects to the meta daemon event stream and deserializes events.
- `internal/types/` — event queues and (multi-cursor) ring buffers.
- `internal/subscriber/`, `internal/subscribermgr/` — subscriber lifecycle and management.
- `pkg/dispatch/` — event dispatch with rate limiting and ignore rules.
- `pkg/subscriber/` — gRPC server exposing the event stream to subscribers, with acks.

## Conventions

- **No `TODO`/`WIP` commits in PRs** — `.github/workflows/no-todo-commits.yml` fails any PR whose
  commit subjects match `TODO`/`WIP`. Such commits must be squashed before merge.
- **Versioning** — binaries/packages are tagged `v8.y.z`; the Go module intentionally stays at
  `v0.8.y`. This is a deliberate choice: `v0` signals the module API is *not* guaranteed stable
  ("a one-way trip" past `v0`), and it lets one module span multiple BeeGFS releases. Patch
  releases use pseudo-versions rather than module tags. See the README FAQ for the full rationale.
- Coding standards and the PR process are documented in the project wiki (linked from
  `CONTRIBUTING.md`); contributions require a signed ThinkParQ CLA.
