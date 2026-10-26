# verifyio

`verifyio` is a Go library for verifiable filesystem IO testing, imported as
`github.com/thinkparq/beegfs-go/verifyio`. Every write produces a block body
that is deterministically generated from a seed and self-checked via
per-stripe CRC32C checksums appended after it — the block itself carries no
descriptive header. Instead, the header (seed, kind, node, offset,
timestamps, etc.) is stored exclusively in an xattr on the same inode, never
embedded in the block data. A verifier can later sweep the file, read back
each block's xattr header and data under a shared range lock, check the stripe
CRCs for internal consistency, then regenerate the body from the xattr's
seed and compare — catching silent corruption, torn writes, and (with a
seed-sensitive `block.Kind`) stale data. What a given `Kind` can actually
detect varies: see its doc comment for the per-kind detection guarantees and
their limits — `zeros`/`ones` in particular ignore the seed entirely and can
never detect a stale read regardless of how the rest of the pipeline is
configured. It backs the standalone `iotest-*` tools documented below and the
`beegfs iotest` CLI command.

> **Scope: this change is the library and its standalone `iotest-*` tools only.**
> The `beegfs iotest` command tree — `ctl/internal/cmd/iotest/` — lands separately.
> Everything below that names `beegfs iotest`, or cites a path beneath it such as
> `ctl/internal/cmd/iotest/DESIGN.md`, is therefore forward-looking: those paths do
> not resolve in this tree and cannot be opened from here. The examples are kept
> because they document the intended shape of the pair, not because the command
> exists yet.

Not every tool needs real xattr support: `iotest-posixbench` (below) never
calls setxattr/getxattr at all. It verifies purely through a run-level
manifest file instead of a per-block xattr, and can optionally embed
per-stripe CRC32C checksums directly in the data (trading verification
depth — no protection against misdirected, torn, or stale writes — for
write throughput). Useful on filesystems without xattr support, or when
benchmarking raw throughput without any per-block metadata overhead.

## How it works

1. **Write**: `xattrstore.Writer.WriteBlock` writes the block header into an
   xattr (`user.verifyio.<offset>-<length>`), then writes the block data —
   deterministic body + per-stripe CRC32C checksums, no embedded header — to the
   data file (intent-before-data ordering). Whether an exclusive range lock is
   held across both is set once, on the Writer, by `WriterConfig.Locking`: soak
   uses `LockExclusive`, while the single-threaded tools that own their file use
   `LockNone` and pay nothing for locking they do not need. The policy is fixed
   at construction rather than chosen per call, because a writer that locks only
   some of its operations offers no guarantee at all — a reader need only land in
   one unlocked window. `LockPolicy` has no default; leaving it unset is an
   error.

2. **Verify**: `verifier.VerifyFile` snapshots all xattrs, builds a sweep-line
   over the file, then for each covered range acquires a shared range lock and
   reads the block back. It classifies every byte range as `CoverageOne` (one
   record, body verified), `CoverageNone` (unwritten), `CoverageMany` (two
   records overlap — a bug), or `CoverageContended` (lock was busy; skipped).

3. **Locking**: `xattrstore.Store` has one locking path, covering three
   scopes with two mechanisms:

   | Scope | Mechanism |
   |---|---|
   | Goroutines in one process | `rangeLockTable` (in-process, per `Store`) |
   | Processes on one node | `F_SETLK` (different pids conflict) |
   | Across nodes | `F_SETLK` → the filesystem's distributed lock manager |

   POSIX record locks are per-process, so `F_SETLK` alone cannot separate two
   goroutines; the range table supplies exactly that scope and nothing else.
   **All threads working on a file must therefore share one `Store` for it** —
   the table is a `Store` field, and two `Store`s in one process see nothing of
   each other while `F_SETLK` will not separate them either, since they share a
   pid. See "Locking" under Design notes below.

## Packages

| Package | Description |
|---------|-------------|
| `block` | Deterministic body + per-stripe CRC32C checksums (the on-disk block); a separate 90-byte header describing it is marshaled here but stored by `xattrstore`, never embedded in the block itself. `MakeBlock`, `VerifyBlock`, body kinds (decimal, PRNG, countup, zeros, ones, repeat). `Verdict` classifies each check; `AllVerdicts()` is the canonical list every consumer must iterate. |
| `xattr` | Thin wrappers around `listxattr`/`getxattr`/`setxattr`/`removexattr` with ERANGE-retry loops. |
| `xattrstore` | Per-offset header storage as xattrs. `Store` (`Put`/`Get`/`ForEachEntry`/`ForEachEntryStrict`/`Overlapping`/`ReplaceRange`/`RemoveCovering`), `Writer` (`WriteBlock`/`Truncate`, which own the data+xattr halves together). `CheckSafeToDestroy` guards a caller-supplied path before a tool clears it. Lock primitives (`TryAcquireExclusive`, `TryAcquireShared`, `Lease`) combine an in-process range table with `F_SETLK`, which the filesystem propagates to its distributed lock manager. **Capacity-limited — see Design notes.** |
| `fileops` | `File` type that manages IO handles per `IOType`. `IOTypeBuffered` implemented; `IOTypeODirect`, `IOTypeMmap`, `IOTypePwritev` are stubs. |
| `verifier` | Sweep-line file verifier. `VerifyFile` with `Options` (ByteRange, LockMode, Log). |
| `trace` | Structured zap logger setup for IO tracing. Deliberately not under `internal/`: the `beegfs iotest` CLI needs it too, though that consumer is not in this change — see the package doc comment for what went wrong while it was internal. |
| `bench/posixbench` | Write-once POSIX IO benchmark with manifest-based verification. |

## Building

Commands below are relative to the `verifyio/` directory (from the repo root,
prefix with `verifyio/`, e.g. `go build ./verifyio/cmd/...`).

Build all tools into `./bin/`:

```sh
go build -o ./bin/ ./cmd/...
```

Build a specific tool into the current directory:

```sh
go build ./cmd/iotest-smoke/
go build ./cmd/iotest-verify/
go build ./cmd/iotest-dump/
go build ./cmd/iotest-util/
go build ./cmd/iotest-posixbench/
```

Install to `$GOBIN` (or `~/go/bin` if `$GOBIN` is unset):

```sh
go install ./cmd/...
```

Run directly without a build step:

```sh
go run ./cmd/iotest-smoke/ -path /tmp/smoke.dat -blocks 100
```

Run tests and benchmarks:

```sh
go test ./...
go test -bench=. ./...
```

## Tools

### iotest-smoke

Single-threaded end-to-end smoke test. Writes N blocks to a file, storing
each block's header in an xattr, reads everything back, and verifies. Useful as a quick
sanity check and as a copy-paste starter for new consumers of the library.
The file is truncated and rewritten on every run.

```
iotest-smoke -path /tmp/iotest-smoke.dat -blocks 1000
iotest-smoke -path /tmp/iotest-smoke.dat -blocks 100 -blocksize 8192 -kind prng
```

| Flag | Default | Description |
|------|---------|-------------|
| `-path` | `/tmp/iotest-smoke.dat` | Data file path |
| `-blocks` | `1000` | Number of blocks to write |
| `-blocksize` | `4096` | Total bytes per block on disk; must satisfy `block.BodyLen`'s body/stripe-CRC split. A multiple of 516 always works; powers of two work **except 65536**, where the total jumps from 65532 to 65540 without landing on it. No header-size floor, since the header lives in the xattr, not the block |
| `-kind` | `decimal` | Body pattern: `decimal`, `prng`, `repeat`, `countup`, `zeros`, `ones` |
| `-iotrace` | `0` | IO trace level (0=off … 5=debug) |
| `-iologfile` | stderr | IO trace destination file |

---

### soak — use `beegfs iotest start soak`

There is no standalone `iotest-soak` binary. It existed, and was removed as a
duplicate: `beegfs iotest start soak` is a strict superset of it.

The two were not a tool and a wrapper — they were two different locking designs.
The standalone opened its own `Store` per handle, which was believed safe on
one node because OFD locks conflict between distinct open file descriptions in
the same process — a guarantee that does not hold on BeeGFS, and the reason
the OFD mode was later removed entirely. The CLI opens one `Store` per *file*,
shared by every worker on it, which is the shape that actually works: POSIX
record locks do not conflict within a process, so the shared `Store`'s
in-process range table provides the goroutine-level exclusion, and the POSIX
locks provide the cross-node exclusion a shared filesystem propagates to its
lock manager.

That made the CLI's design a superset — a run without `--nodes` is a single-node
run — and left the standalone as a second implementation of the same tool,
already drifted in its duration default, its flag names, and its exit code on a
clean Ctrl+C. It was deleted rather than reconciled.

```
beegfs iotest start soak --path /mnt/beegfs/soak --threads 8 --duration 5m
beegfs iotest start soak --path /mnt/beegfs/soak --nodes node1,node2 --blocksize 4096
```

Thread 0 operates exclusively on a `.noOverlap` file; the remaining N−1 threads
share a pool of ⌊(N−1)/2⌋ `.shared` files (clamped to at least one whenever
there is a shared worker, so `--threads 2` uses a single shared file). Each
worker loops for the requested duration, randomly picking a file from its pool,
an operation (write or range-verify), and a block-aligned offset. The run aborts
immediately if body corruption (`CoverageMany`, or `Verdict != OK`) is detected.

Run `beegfs iotest start soak --help` for the current flags; see
`ctl/internal/cmd/iotest/DESIGN.md` for the multi-node coordination design.

File layout for `--threads 8`:

```
<path>/iotest-soak-0.noOverlap   ← thread 0 only
<path>/iotest-soak-0.shared      ← shared by threads 1–7
<path>/iotest-soak-1.shared
<path>/iotest-soak-2.shared
```

---

### iotest-verify

Verifies all blocks in a file written by `iotest-smoke` or `beegfs iotest start soak`.
By default prints only anomalous spans; `-verbose` prints every span.

```
iotest-verify -path /tmp/iotest-smoke.dat
iotest-verify -path /tmp/soak/iotest-soak-0.noOverlap -verbose
```

| Flag | Default | Description |
|------|---------|-------------|
| `-path` | (required) | Data file to verify |
| `-verbose` | false | Print all spans, not just anomalies |

**Verdicts.** This is a forensic, post-workload tool, so its job is to say what
it found *and* how much it actually looked at. The two are reported separately:
one exit code per verdict, so a wrapper can branch on the code alone.

| Exit | Verdict | Meaning |
|------|---------|---------|
| 0 | `PASS` | At least one record verified, no anomalies |
| 1 | `FAIL` | An anomaly, including a malformed xattr record name |
| 2 | — | Usage |
| 3 | `INCOMPLETE` | No record verified because every record's span was contended |
| 4 | `NO_DATA` | No record verified and nothing was skipped — nothing to verify |
| 5 | `ERROR` | The sweep did not complete |

Only `PASS` means the data was checked and is good. `INCOMPLETE` is the one
verdict that means *ask again* — a lock held the tool off, which for a
post-workload sweep means it ran too early, not that anything is wrong.

Coverage is counted in **records**, never in spans. A gap span's bytes are read,
but no record claims them, so reading them proves nothing about written data and
never counts toward coverage.

The last line of stdout is always a machine-readable summary with a fixed field
set, printed on every exit path including the ones that end before a sweep
completes:

```
verdict=PASS records=12 gaps=3 contended=0 anomalies=0 path=/tmp/iotest.dat
```

`path` is last because it may contain spaces: everything after `path=` is the
path. Parse this line rather than grepping the prose above it, which is meant
for people and has changed wording before.

---

### iotest-dump

Dumps the xattr index of a data file in human-readable form. Lists every
stored record sorted by offset, with offset, length, worker ID, cycle,
body-pattern kind, timestamp, and verification verdict.

```
iotest-dump -path /tmp/iotest-smoke.dat
iotest-dump -path /tmp/soak/iotest-soak-0.shared
```

| Flag | Default | Description |
|------|---------|-------------|
| `-path` | (required) | Data file to inspect |

---

### iotest-util

Developer utility for library experiments, with the operation as the first
positional argument. `-path` is required and has no default.

```
iotest-util -path /mnt/beegfs/test.dat -blocksize 1024 -blocks 64 write
iotest-util -path /mnt/beegfs/test.dat -blocksize 1024 -blocks 200 xattr-capacity
```

| Operation | Description |
|-----------|-------------|
| `write` | Write blocks through `xattrstore.Writer`, without the verify pass `iotest-smoke` does. The smallest possible driver of the write path |
| `xattr-capacity` | Write one xattr per block offset until `setxattr` fails, reporting how many the filesystem accepted |

| Flag | Default | Description |
|------|---------|-------------|
| `-path` | (required) | Data file. **Both operations clear it** — see the note below |
| `-blocksize` | `1024` | Bytes per block |
| `-blocks` | `64` | Number of blocks |

`xattr-capacity` deletes and recreates its file rather than truncating it, since
`O_TRUNC` clears data but leaves xattrs behind, which would let overwrite-style
puts succeed past the real limit and report a wrong answer. It removes the file
again when the run ends: the answer is the printed count, and a leftover artifact
carries more xattr names than `listxattr` can return, which made the next run
refuse to start on its own leavings.

Both operations refuse a `-path` that does not look like a verifyio artifact
(`xattrstore.CheckSafeToDestroy`), so a slip like `-path /etc/passwd` is rejected
rather than obeyed.

Note the number `xattr-capacity` reports is the **setxattr** limit, which is
often not the one that binds — see Capacity below.

---

### iotest-posixbench

Write-once POSIX IO benchmark with manifest-based post-run verification.
Does not use xattrs; records a `posixbench.json` manifest alongside the
data so the verify subcommand can check body integrity later.

```sh
# Write 1 GiB per worker across 8 threads, then read back:
iotest-posixbench run -path /mnt/testdir -threads 8 -read

# Verify after the fact (reads posixbench.json from the same directory):
iotest-posixbench verify -path /mnt/testdir
```

**`run` flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-path` | (required) | Target directory |
| `-threads` | `4` | Worker goroutines |
| `-block-size` | `4194304` (4 MiB) | IO transfer size in bytes |
| `-file-size` | `1073741824` (1 GiB) | Per-worker data size in bytes (bytes, not a size suffix) |
| `-files-per-worker` | `1` | Files per worker; ignored when `-layout n-to-1` (all workers share the one file regardless) |
| `-layout` | `1-to-1` | `1-to-1` (one file per worker) or `n-to-1` (all workers share one file) |
| `-kind` | `decimal` | Body pattern: `decimal`, `prng`, `zeros`, `ones`, `countup`, `repeat` |
| `-seed` | `0` | Pattern seed (0 = random, recorded in manifest) |
| `-read` | false | Run a sequential read phase after writing |

**`verify` flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-path` | (required) | Directory containing `posixbench.json` |
| `-verbose` | false | Print all anomalies (default: summary only) |

---

## Building a tool on verifyio: bound your own waits

Every `verifyio` call (`Writer.WriteBlock`, `verifier.VerifyFile`, etc.) is
purely synchronous with no internal cancellation. This is deliberate — Go
cannot interrupt a blocked syscall, so there's no clean way to bolt
cancellation onto an in-flight `read`/`write`/`fsync`/lock call — but it means
a call can, in principle, never return. For a library whose whole purpose is
finding filesystem bugs, that's not a hypothetical edge case: a call hanging
forever may be exactly the bug a tool is meant to catch.

**Any tool that waits on such a call — directly, or via a worker goroutine
doing so — must wrap that wait in a bounded watchdog, not an unbounded
`wg.Wait()`/`<-done`.** Use `verifyio/watchdog` (a public package, not
`internal`, specifically so tools outside the `verifyio` tree -- e.g.
`ctl/internal/cmd/iotest`'s `soak` command -- can depend on it too):

- `watchdog.Wait(ctx, grace, done)` races a done-channel against a grace
  period measured from when `ctx` becomes done (by any means — a deadline,
  an anomaly, a signal). Returns `true` if it gave up waiting; the caller
  should report a distinct **STUCK** outcome (not PASS/FAIL) and exit soon —
  the process exiting is what actually reclaims the abandoned goroutine(s),
  since nothing can stop them directly.
- `watchdog.Activity` lets each worker record what it's doing right now
  (`Start`/`Done`), so a watchdog firing can report *which* worker is stuck
  and on what op/file/offset, rather than just "something, somewhere".

`ctl/internal/cmd/iotest`'s `soak` command is the reference implementation
(it lives outside the `verifyio` tree, which is exactly why this package
isn't `internal`): a fixed 3-minute shutdown grace, cancellation shared by
the duration timeout, a stop signal and an anomaly alike, and a
`Snapshot()`-driven STUCK report. That branch also carries a watcher on
`xattrstore.AbandonedLocks()`; the registry was removed here once any release
error became fatal, so the first abandoned syscall ends the run rather than
accumulating. Reconciling the two is a TODO for when the ctl work lands --
see `TODO.md`.

A second implementation used to live at `cmd/iotest-soak`, with its own
`-shutdown-grace` flag and a 30-second default. It was deleted as a duplicate
-- see the soak section above for why the two were not interchangeable.

If a new tool is short-lived/single-shot and always run interactively (a
human will notice a hang and Ctrl-C), it's fine to consciously skip this —
but say so in the tool's own doc comment. The decision should be explicit,
not an oversight.

---

## Design notes

**Intent-before-data**: the xattr is always written before the data block.
After a crash, a verifier may see an xattr with no matching data (recoverable)
but never data without a matching xattr.

**Locking**: `F_SETLK` is the only kernel mechanism used, so exclusivity is
enforced by the filesystem's distributed lock manager (e.g. BeeGFS) across
every node sharing the data file. Two complications it has to account for:

- POSIX locks are scoped to `(process, inode)`, not to a specific open file
  description — two goroutines in the same process calling `F_SETLK` on
  overlapping ranges never conflict with each other. A `rangeLockTable`
  provides that missing goroutine-level exclusivity: a non-blocking,
  in-process check consulted before every `F_SETLK` call, never held across a
  syscall. It conflicts on **any** overlap, shared-against-shared included, so
  two goroutines on one `Store` cannot hold overlapping *read* locks; the loser
  gets `ErrLockBusy`. That is deliberate — a POSIX read lock is per-process
  too, so the two would share one kernel lock state and either one's release
  would destroy it under the other. Overlapping shared locks across processes
  and across nodes are unaffected, which is what shared locks are for.

  An earlier design used OFD locks (`F_OFD_SETLK`) for the goroutine scope,
  on the strength of their defining guarantee: they are owned by the open file
  description, so two descriptions in one process conflict. **That guarantee
  does not hold on BeeGFS.** Measured on a three-node cluster 2026-08-17:
  BeeGFS keys record locks by `(node, pid)` and ignores the open file
  description even for `F_OFD_SETLK` commands, so two fds in one process both
  get the lock, and either one's unlock destroys the other's. Cross-node and
  cross-process OFD enforcement worked correctly — the mode bought nothing the
  other two mechanisms did not already buy, and carried a false model of the
  filesystem, so it was removed. Whether BeeGFS's behaviour here is intended is
  an open question against `beegfs-core`; see `TODO.md`.
- `F_SETLK` is documented as non-blocking, but a slow or unresponsive
  distributed lock manager can make it — or the xattr scans used to compute
  the lock range — block indefinitely, and Go cannot interrupt a blocked
  syscall. `withTimeout` bounds how long a caller waits before giving up
  (`ErrLockTimeout`), but the abandoned goroutine keeps running in the
  background with an unknown eventual outcome. Every current caller treats that
  as fatal and unwinds — a release error is never superseded by a benign one
  (see `ReleaseErrorOrCause`) and `VerifyFile`'s sweep aborts on the first span
  error — so a run ends rather than accumulating abandonments. Nothing marks the
  `Store` itself unusable, though, so a caller that keeps going gets no help
  from the library; what a long-running soak should report is open — see
  `TODO.md`. Because
  that outcome is unknown, the `rangeLockTable` guard for a timed-out call is
  deliberately **not** freed (see `guardSafeToRelease`) — freeing it
  would let another same-process goroutine wrongly believe the range is free
  and re-acquire it, only for the real, delayed syscall to land later and
  silently invalidate that second goroutine's lock too.

**Capacity — a file can only hold a few thousand records**: storing one xattr per
block puts a hard ceiling on how much of a file `verifyio` can describe, and the
ceiling is low. Measured with 4 KiB blocks:

| Filesystem | What binds first | Records | Data per file |
|------------|------------------|---------|---------------|
| tmpfs | `listxattr` (E2BIG) | 2437 | 9 MiB |
| ext4 | `setxattr` (ENOSPC) | 31 | 124 KiB |

Two different limits, and which one binds depends on the filesystem. `setxattr`
runs out of per-inode xattr space, which is filesystem-specific — ext4 has a
single-block budget, tmpfs accepted 44,000+. `listxattr` cannot retrieve a name
list larger than `XATTR_LIST_MAX` (65536 bytes), and **that one is a kernel limit
enforced in the VFS**, so it is the same everywhere including BeeGFS: a generous
per-inode xattr budget cannot be reached through it.

The record count is roughly constant because it is driven by *name* length, so
the data a file can cover scales with block size: only the 4 KiB row above is
measured, but the same ~2400-record budget implies roughly 1.2 MiB at 512 B
blocks and roughly 1.9 GiB at 1 MiB. Larger blocks are the cheap mitigation.

This applies to writes as much as reads: `Overlapping` lists the whole namespace
on every call, so `ReplaceRange` (and therefore `WriteBlock`), `Truncate`,
`TryAcquireExclusive`, and every verifier sweep all fail past the ceiling. A run
that exceeds it does not degrade — it stops partway through with `argument list
too long` on a filesystem with terabytes free.

Lifting this needs a store that writes records to a separate file instead of the
inode's xattrs, with this one kept as a selectable mode. Until then, size runs
against the table above.

**Writes go through `ReplaceRange`, not `Put`**: `Writer.WriteBlock` writes its
record with `store.ReplaceRange`, which removes every record overlapping the new
extent before writing it (replace-and-shred).

`Put` alone is not enough. `Put` is keyed by `(offset, length)`, so it replaces
in place only when the *extent is identical*. That holds while the block size never changes —
each write to the same position reuses the name
`user.verifyio.<offset>-<blocksize>` — but a re-run at a different `--blocksize`
writes a new name and leaves the old record in place beside it. The verifier
then sees two records claiming the same bytes and reports `CoverageMany`, which
reads as the filesystem having let two writers claim one range: the most
alarming thing this tool can say, for what is only a changed flag.

`ReplaceRange` covers only the xattr half. `WriteBlock` also zeroes the bytes a
shredded wider record freed, since those still hold whatever was written there
before and would otherwise read as stale data in a span that claims to be
unwritten. That zeroing happens *after* the data write, so a crash in between
leaves uncovered bytes (flaggable and recoverable) rather than a record with no
data — the same trade as intent-before-data above.

`Put` does survive in one spot: the post-`fsync` re-stamp that sets `TagFsynced`
on the header, which rewrites the same extent while the lock is still held.

**Verifier races under concurrent writes**: `verifier.VerifyFile` snapshots the
set of xattr entries (offset/length) before acquiring per-span locks, but
**re-reads each header under the shared lock** before verifying. When the writer
was built with `xattrstore.LockExclusive`, it holds an exclusive lock across both
the xattr write and the data write, so a verifier holding the shared lock sees a
mutually consistent header/body pair — a block reseeded after the snapshot is
verified against its new header and passes, rather than being falsely reported as
`BODY_CORRUPT`.

That guarantee is conditional on the writer's `LockPolicy`. Against a `LockNone`
writer the verifier can observe a partially applied write, and what it reports is
a true statement about what was on disk at that moment.

The record itself does carry a one-way signal of which policy wrote it:
`TagFsynced` is stamped only inside the `LockExclusive` branch, after the fsync
and while the lock is still held, so a record with the bit set proves an
exclusive lock spanned both the xattr write and the data write. The converse does
not follow — a `LockExclusive` writer that died between the data write and the
re-stamp leaves the bit clear — so treat it as evidence when present and as
nothing when absent. `verifier.logSpan` surfaces it as `fsynced`.

Two races are benign even under `LockExclusive` and are not anomalies:
(1) a write completing entirely after the snapshot produces apparent data with no
xattr claim (`CoverageNone, AllZero=false`); (2) an entry removed or replaced
between the snapshot and the per-span lock is reported `CoverageContended` (its
header was gone when the lock was held).

**Consumers disagree about case (1), deliberately.** `beegfs iotest start soak`
treats it as benign and flags only `CoverageMany` and `Verdict != VerdictOK`. The
standalone `iotest-verify` and `beegfs iotest verify` treat it as a hard FAIL,
because they are usually run against a quiescent file where unclaimed non-zero
bytes really are the anomaly the tool exists to find. Know which policy applies
before reading a report: a FAIL from `iotest-verify` against a file a soak is
actively writing may be this race rather than a finding.

**The shred window is the sharp edge here**, and it needs no second writer at
all. `WriteBlock` removes the records it is replacing *before* it writes the data
and *before* `zeroShredded` clears what the shred freed. Between those steps the
freed bytes are non-zero and claimed by nothing, which is byte-for-byte
indistinguishable from the anomaly — the store truthfully reports no record, the
disk truthfully reports non-zero. Re-reading the store cannot tell the two apart,
because both reads are accurate.

The remedy is a shared range lock over the span, taken the way `oneSpan` takes
one — **but only against a `LockExclusive` writer.** These locks are advisory:
`Writer.takeLockIfNeeded` is a no-op under `LockNone`, so a verifier-side lock
excludes nothing there and the window stays open. Measured at roughly 3% of sweeps against
a locking writer that changes block size (and so shreds), and 0% once the verifier
locks the span first. Those numbers came from a prototype driven by soak, which is
the only `LockExclusive` writer; every writer in this package is `LockNone`, so
they are not reproducible from this branch alone.

`noneSpan` does not take that lock, and carries a known limitation documented at
the function: when its recheck finds exactly one record it re-dispatches the whole
span, so a record covering only part of it makes the remainder report as covered.
No in-tree code path runs a sweep concurrently with a writer, so nothing here
reaches it — note that is a statement about how the tools are used, not about
`LockNone`, which if anything widens the window rather than closing it. The fix is
the shared range lock, alongside a writer that actually takes locks.
