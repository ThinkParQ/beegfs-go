# iotest package design

`beegfs iotest` is a family of IO testing tools for BeeGFS filesystems. The
package is split into two groups: **standalone commands** that run locally
without coordination, and **framework-managed workloads** that support
multi-node SSH fan-out, lifecycle management, and a shared status model.

---

## File map

| File | Role |
|------|------|
| `iotest.go` | Command registration entry point |
| `framework.go` | Core framework: lifecycle, status, SSH fan-out |
| `nodes.go` | Node parsing and SSH launch helpers |
| `trace.go` | Zap-based IO trace logger (used by soak, smoke) |
| `dump.go` | Standalone: inspect a single data file |
| `verify.go` | Standalone: verify data files written by soak/smoke |
| `smoke.go` | Standalone: single-node write+verify sanity check |
| `bench.go` | Framework workload: bandwidth benchmark |
| `soak.go` | Framework workload: long-running integrity stress test |
| `inval.go` | Framework workload: cache invalidation measurement |

---

## Entry point

`iotest.go` wires everything up. Standalone commands (`smoke`, `verify`,
`dump`, `bench run/verify`) are added directly. Framework-managed workloads
(`soak`, `bench`, `inval`) are passed to `NewFrameworkCmds`, which
generates the shared `start` / `status` / `stop` subcommands automatically.

---

## Framework (`framework.go`)

### Tool interface

Every framework-managed workload implements `Tool`:

```go
type Tool interface {
    Name() string
    Run(ctx context.Context, cfg RunConfig) error
}
```

Tools are intentionally dumb: they run work, increment a counter, and respect
the stop signal. Everything else — SSH fan-out, status files, table rendering,
wait/watch — is handled by the framework.

Five optional interfaces extend the base:

| Interface | Method | Purpose |
|-----------|--------|---------|
| `FlagProvider` | `RegisterFlags(*cobra.Command)` | Register tool-specific flags on `start`; the framework forwards them verbatim to remote nodes |
| `Describer` | `Description() string` | One-line description in `start` help text |
| `StatusFormatter` | `FormatExtraStatus(node string, data json.RawMessage) string` | Render tool-specific JSON below the status table |
| `CompletionHinter` | `CompletionHint(path string) string` | Print post-run guidance after `--wait`/`--watch` completes |
| `StatusFootnote` | `FormatStatusFootnote() string` | Print a one-time explanatory footer after all per-node `FormatExtraStatus` blocks |

### RunConfig

The framework passes this to `Tool.Run`:

```go
type RunConfig struct {
    Path           string
    TotalOps       *atomic.Int64   // increment once per completed operation
    StopSignal     *atomic.Bool    // poll; return nil when true
    SetExtraStatus func(v any)     // write tool-specific JSON into the status file
}
```

### Status files

Each running workload writes a `.iotest-<workload>-<node>.json` file to
`--path`. The format is `workloadStatus`:

```
node, workload, status ("running"|"done"|"failed"),
startTime, endTime, totalOps, error, extraData (opaque JSON blob)
```

The framework writes an initial `running` record on start, ticks a 5-second
update goroutine while running, and writes a final `done`/`failed` record when
`Tool.Run` returns. The update goroutine is explicitly stopped and drained
before writing the final record so it cannot race and overwrite "done".

A separate `.iotest-invocation.json` records `os.Args` and start time; it is
excluded from the status glob and shown as a header line in status output.

### Stop signal

`beegfs iotest stop` creates `.iotest.stop` in `--path`. The framework's
`watchStopSignal` goroutine polls for this file every second and sets
`RunConfig.StopSignal`. Tools are expected to check this and return `nil`
cleanly when it is set.

### Local execution (`startLocal`)

Runs all selected tools concurrently in goroutines on the local node. Refuses
to start a workload that is already running (checks status files for
`status=running` on the local hostname). Clears any stale stop signal before
starting.

### Multi-node execution (`startMultiNode`)

Fans out one SSH session per (tool × node) pair concurrently. Each session
runs `nohup beegfs <args> > .iotest-<workload>-<node>.log 2>&1 &` and returns
immediately — the framework does not wait for completion. Status is tracked
through the shared status files on the BeeGFS path.

`buildRemoteArgsForWorkload` strips `--nodes` (prevents recursion),
`--fresh` (already cleaned before fan-out), and other workload names from
positional args so each remote session runs a single workload.

### Wait / Watch

`--wait` polls silently and prints the table once on completion.
`--watch` redraws the table on each tick using a terminal refresher.
Both call `util.TerminalAlert()` when all workloads reach a terminal state.

### `--fresh`

Before starting, removes all `iotest-*` and `.iotest-*` files from `--path`.
A blocklist of system directories (`/`, `/tmp`, `/home`, etc.) prevents
accidental deletion. The clean happens on the invoking node only, before
SSH fan-out.

---

## Node helpers (`nodes.go`)

`parseNodes` accepts `-N` as either a comma-separated list or a file path
(newlines or commas as delimiters, `#` comments). Produces `[]NodeSpec`.

`sshStartBackground` launches the remote process via
`ssh -o BatchMode=yes <node> 'nohup beegfs ... > <log> 2>&1 &'`. It returns
as soon as the background process is launched. `shellJoin` single-quotes each
argument for safe shell interpolation.

---

## Standalone tools

### `smoke.go`

Single-node write+verify using the `block`/`xattrstore`/`verifier` integrity
stack. Writes N blocks, verifies them immediately, cleans up. Quick sanity
check that integrity machinery works on a given filesystem. Supports an
`--iolog-level` zap trace logger via `trace.go`.

### `verify.go`

Accepts a file or directory (globs `iotest-soak-*`). For each file: checks
xattrs are present (prints a helpful error with BeeGFS-specific advice if
not), then calls `verifier.VerifyFile` which produces `Span` objects
classified by coverage:
- `one` — exactly one record covers this region; verifies body CRC and xattr match
- `none` — uncovered region; fails if non-zero bytes are present
- `many` — overlapping records; always a failure
- `contended` — lock held; skipped

Prints anomalies always; clean spans only with `--verbose`.

### `dump.go`

Opens a single data file, reads all xattr records, sorts by offset, prints
each block's full header (kind, worker, cycle, node, timestamp, seed, CRC)
and a `VerifyBlock` verdict. Useful for hand-inspecting specific files.

---

## Framework workloads

### `bench.go`

Bandwidth benchmark. Multiple workers write fixed-size files; reports MiB/s.
Implements `StatusFormatter` to show live bandwidth in the status table.
Has a companion standalone `bench verify` subcommand.

### `soak.go`

Long-running integrity stress test. Workers write and re-write blocks to
overlapping and non-overlapping files using the xattrstore integrity stack.
Verifies global file locks are configured when running cross-node. Uses zap
for IO tracing. The canonical tool for finding silent corruption or torn
writes under sustained load.

**Relationship to `verifyio/cmd/iotest-soak`:** these are two independent
implementations, not one wrapping the other -- `iotest-soak` is `package
main` (a standalone binary), so this package cannot import it as a library.
Both are consumers of the same underlying integrity machinery
(`verifyio/block`, `xattrstore`, `fileops`, `verifier`); the worker-spawning,
op-selection, and file-pool logic is separately implemented in each rather
than shared.

The one behavioral difference that isn't incidental: `iotest-soak` opens its
stores via `xattrstore.OpenStore` (`LockModeOFD`, single-node only), while
this file's `soakOpenStores` uses `xattrstore.OpenStoreMultiNode`
(`LockModePOSIX`). That follows directly from what each tool is for --
`iotest-soak` is the original, simpler tool for direct single-node testing
of the library (see `verifyio/README.md`), while `SoakTool` here exists
specifically to run across a real multi-node BeeGFS cluster via the
framework's SSH fan-out, which is why it needs the cross-node lock mode
(and therefore `tuneUseGlobalFileLocks=true` -- see `soakCheckGlobalFileLocks`)
in the first place.

### `inval.go`

Measures cache invalidation latency between nodes. Leader/follower roles are
claimed via `O_EXCL` on a lock file on `cfg.Path` (a one-time, not
per-round, rendezvous). The leader mutates the shared test file each round
and the follower measures how long BeeGFS takes to propagate the
invalidation. Writes a structured log file per node with per-round stats.

Per-round coordination (the ready handshake, round announcements, and
observation reports) goes over a plain TCP connection to the leader
(`invalControlPort`), not through files on `cfg.Path`. It used to: a round
file's mere existence is itself subject to the same dentry/ENOENT-cache TTL
being probed, so a follower's single revalidation of the test directory
could refresh both the round file and the test file's mutated attribute in
one round-trip — silently absorbing real invalidation latency into the
(unmeasured) round-file wait and reporting near-zero latency for a case that
was actually slow. This is a well-known failure mode in coordination-channel
design generally, and inval.go's fix follows the standard principle:
coordination must not ride the same cache/TTL regime as the thing being
measured. Only the actual test-file mutations (and the one-time leader lock)
still live on `cfg.Path`.

The `chmod` op encodes the expected mode as `0600 | (round % 64)` rather
than a fixed `^0077` toggle. XOR-with-a-constant is its own inverse, so it
used to flip between exactly two mode values with period 2 — a follower
reading a one-round-stale cached mode would see it match the *current*
round's expected value, reporting a false "instant visibility" for what
could be a real caching regression. `os.FileMode.Perm()` is only 9 bits, so
a fully non-repeating encoding isn't possible across `invalDefaultRounds`
(1000) rounds; varying the low 6 (group+other) bits raises the alias period
from 2 rounds to 64, adopting the standard "N mod K, K large" fallback
for permission-based signals when true monotonicity isn't available. The owner `rw` bits (`0600`) are held fixed:
leader and follower run as the same uid, and an owner-unreadable or
-unwritable mode would break `setxattr`/`getxattr` the next time
`invalOpXattr` is picked for the same file.

---

## File naming conventions

| Pattern | Purpose |
|---------|---------|
| `.iotest-<workload>-<node>.json` | Per-node workload status |
| `.iotest-<workload>-<node>.log` | SSH stdout/stderr capture (framework) or explicit log (inval) |
| `.iotest-invocation.json` | Full `os.Args` + start time of the invoking command |
| `.iotest.stop` | Stop signal; presence triggers all workloads to exit |
| `iotest-soak-*` | Soak data files (no leading dot; subject to `--fresh` cleanup) |

---

## Future investigation (soak cross-node anomalies)

Cross-node soak runs surface `verdict=BODY_CORRUPT` anomalies that are cache
coherence effects, not corruption. The verdict distinguishes them: `BODY_CORRUPT`
means the block passed its own in-data stripe CRCs (internally consistent) but
does not match the seed in the current xattr header — a coherent *older* version,
i.e. a stale read. Genuine corruption would be `BODY_CRC_MISMATCH`. Two open
items, deliberately deferred past the first check-in:

1. **Fail-vs-warn split keyed on `fsynced`.** Soak currently treats every
   `BODY_CORRUPT` as fatal. But the writer syncs and stamps `TagFsynced` in the
   xattr *before* releasing its exclusive lock (see `xattrstore.Writer.WriteBlock`),
   so the two cases differ:
   - `fsynced=true` + stale body → a durably-committed write came back stale.
     This is the real coherence red flag → keep as **fail**.
   - `fsynced=false` + stale body → the reader observed a not-yet-committed
     (intent-phase) block → should probably be a **warn/counter**, not a hard
     failure.

2. **Why is `fsynced=false` ever visible to a reader?** Under the intended lock
   protocol it should be impossible: the writer flips `fsynced`→true and re-puts
   the xattr while holding the exclusive lock, and a reader must take a conflicting
   shared lock. Observing the transient intent state implies either (a) the
   reader's `getxattr` is served from a stale client metadata cache (POSIX lock
   acquisition does not invalidate it), or (b) a cross-node lock-exclusivity gap
   (shared lock granted while the writer holds exclusive). Both are reader-side
   and distinct from the data-page-cache staleness. A disambiguating diagnostic
   (on mismatch, immediately re-read the xattr and note whether the shared-lock
   acquisition contended) would tell (a) from (b). The writer's fsync-before-unlock
   is already correct — no writer change is implied.
