# verifyio — work in progress

## Background

The verifyio library was migrated into `beegfs-go` from a standalone project
and now lives at the top-level `verifyio/` package, consumed by the standalone
`iotest-*` tools and the `beegfs iotest` CLI.

### What's done

- `verifyio/{block,verifier,xattr,xattrstore,fileops}/` — xattr-backed block writer/verifier, OFD locking, sweep-line verifier
- `verifyio/bench/posixbench/` — sequential POSIX write/read benchmark with manifest
- `verifyio/internal/trace/` — zap IO trace logger
- `verifyio/cmd/` — CLI tools: iotest-smoke, iotest-soak, iotest-verify, iotest-dump, iotest-posixbench, iotest-util
- All tools show usage+examples when invoked without arguments
- `iotest-verify` gives a clear error when a file has no xattr records (catches BeeGFS missing `user_xattr`)
- `iotest-soak` redesigned: two file pools (noOverlap + shared), OFD locking, extensible op system, duration-controlled loop
- All tools already participate in `go test ./...`, `go vet ./...`, staticcheck via `make test`
- No Makefile targets added — consistent with the `watch/cmd/test-*` precedent (dev tools use `go install` directly)

---

## Next: new `beegfs iotest` subcommand

> **Status: implemented.** The `beegfs iotest` command now exists
> (`ctl/internal/cmd/iotest`, see its `DESIGN.md`). The notes below are retained
> for the original design rationale.

The goal is a new subcommand under the `beegfs` CLI that exposes iotest
functionality to users running against a live BeeGFS mount.

### Open design questions

1. **Client-side or server-side IO?** ✅ DECIDED: client-side.
   The CLI process does IO via POSIX through the BeeGFS mount, exercising the full path
   (client module → network → storage). No new daemon-side handlers needed.

2. **Relationship to `beegfs benchmark`**
   - New sibling subcommand (e.g. `beegfs iotest`) — separate concern, different flags?
   - Or extend `beegfs benchmark` with a client-side mode?

3. **Which iotest mode first?**
   - Integrity mode (`iotest-soak` style) — write+verify with xattrs, detects silent corruption
   - Benchmark mode (`iotest-posixbench` style) — sequential write/read, reports MB/s
   - Or both under the same subcommand with subcommands like `beegfs iotest bench` / `beegfs iotest verify`?

4. **How the CLI plumbs into iotest**
   - The `ctl/pkg/ctl/` layer is the right place for business logic
   - `ctl/internal/cmd/` for Cobra command tree and output formatting
   - The verifyio library packages are already importable as `github.com/thinkparq/beegfs-go/verifyio/...`

### Suggested starting point (to confirm in the morning)

A `beegfs iotest` command with two subcommands mirroring posixbench:
- `beegfs iotest run` — write data files to a target path (flags: threads, file-size, block-size, kind)
- `beegfs iotest verify` — check data files against the manifest

This gives a client-side benchmark with built-in post-run correctness check,
which is the main thing `beegfs benchmark` cannot do today.

---

## Multi-node coordination (future TODO, 2026-06-10)

Today `iotest-soak` uses OFD/fcntl locking for cross-goroutine coordination on shared files.
That's an important test scenario, but HPC apps commonly use other coordination mechanisms
(MPI ranks, job scheduler decomposition, etc.) and the default mode should be lock-free for
performance reasons. Three options were discussed for multi-node iotest:

### Option A: Static partition (recommended default)

Each worker is given `--node-id N --total-nodes M`. Block ownership is pure math:

    globalWorkerID = N * threadsPerNode + threadID
    owns block i  ⟺  i % totalGlobalWorkers == globalWorkerID

No runtime coordination. Maps directly onto SLURM env vars (`$SLURM_NODEID`, `$SLURM_NNODES`).

- Pro: zero overhead; mirrors how MPI apps actually decompose work
- Pro: trivially launchable from any job scheduler
- Con: can't test cross-node write contention (by design)
- Con: misconfigured overlap is caught by the verifier after the fact, not before

Implementation note: `opRunner.runOnce` currently samples `rng.Int63n(nBlocks)`.
Change to sample from an owned-block filter: `blockIdx % totalWorkers == globalWorkerID`.
For large files, express as modular arithmetic rather than a pre-built slice.

Open question: `--node-id / --total-nodes` (ergonomic for scheduler) vs.
`--global-worker-id / --total-global-workers` (flat, simpler for opRunner)?
Flat form is simpler internally; scheduler form is more natural to specify.

### Option B: Leader-follower work dispatch

One process maintains a work queue; followers pull `(file, offset)` assignments.
Leader can hand out non-overlapping work OR deliberately create contention.

- Pro: can test contention and non-contention dynamically; leader detects dead followers
- Con: significant complexity — needs a transport (gRPC listener or coordination file on the mount)
- Con: leader is a throughput bottleneck for high-frequency small ops
- Best fit: a future `--mode=contention` flag, not the default path

### Option C: Rendezvous file on the BeeGFS mount (variant of A)

Each node writes `<testdir>/roster/<hostname>.ready`, then polls until it sees
`--total-nodes` files. Once all are present, each node independently computes its
partition using the same static math as Option A. Eliminates the need to pass
`--node-id` explicitly.

- Pro: self-describing — the FS under test is also the coordination plane
- Pro: no pre-assigned node IDs; hostname-derived
- Con: adds a barrier at startup; fragile if a node dies before writing its ready file
- Best fit: ergonomic convenience layer on top of Option A once A is working
