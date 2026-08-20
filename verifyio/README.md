# verifyio

`verifyio` is a Go library for verifiable filesystem IO testing, imported as
`github.com/thinkparq/beegfs-go/verifyio`. Every write produces a block body
that is deterministically generated from a seed and self-checked via
per-stripe CRC32C checksums appended after it — the block itself carries no
descriptive header. Instead, the header (seed, kind, node, offset,
timestamps, etc.) is stored exclusively in an xattr on the same inode, never
embedded in the block data. A verifier can later sweep the file, read back
each block's xattr header and data under a shared OFD lock, check the stripe
CRCs for internal consistency, then regenerate the body from the xattr's
seed and compare — catching silent corruption, torn writes, and stale data.
It backs the standalone `iotest-*` tools documented below and the
`beegfs iotest` CLI command.

Not every tool needs real xattr support: `iotest-posixbench` (below) never
calls setxattr/getxattr at all. It verifies purely through a run-level
manifest file instead of a per-block xattr, and can optionally embed
per-stripe CRC32C checksums directly in the data (trading verification
depth — no protection against misdirected, torn, or stale writes — for
write throughput). Useful on filesystems without xattr support, or when
benchmarking raw throughput without any per-block metadata overhead.

## How it works

1. **Write**: `xattrstore.Writer.WriteBlock` acquires an exclusive OFD range
   lock, writes the block header into an xattr (`user.verifyio.<offset>-<length>`),
   then writes the block data — deterministic body + per-stripe CRC32C
   checksums, no embedded header — to the data file before releasing the
   lock (intent-before-data ordering).

2. **Verify**: `verifier.VerifyFile` snapshots all xattrs, builds a sweep-line
   over the file, then for each covered range acquires a shared OFD lock and
   reads the block back. It classifies every byte range as `CoverageOne` (one
   record, body verified), `CoverageNone` (unwritten), `CoverageMany` (two
   records overlap — a bug), or `CoverageContended` (lock was busy; skipped).

3. **Locking**: OFD locks (`F_OFD_SETLK`) are per-open-file-description, so
   two goroutines each holding their own `*os.File` to the same path serialize
   against each other — the same as two separate processes.

## Packages

| Package | Description |
|---------|-------------|
| `block` | Deterministic body + per-stripe CRC32C checksums (the on-disk block); a separate 90-byte header describing it is marshaled here but stored by `xattrstore`, never embedded in the block itself. `MakeBlock`, `VerifyBlock`, body kinds (decimal, PRNG, countup, zeros, ones, repeat). |
| `xattr` | Thin wrappers around `listxattr`/`getxattr`/`setxattr`/`removexattr` with ERANGE-retry loops. |
| `xattrstore` | Per-offset header storage as xattrs. `Store` (Put/Get/ForEachEntry/Overlapping/ReplaceRange), `Writer` (WriteBlock with OFD locking), OFD lock primitives (`TryAcquireExclusive`, `TryAcquireShared`, `Lease`). |
| `fileops` | `File` type that manages IO handles per `IOType`. `IOTypeBuffered` implemented; `IOTypeODirect`, `IOTypeMmap`, `IOTypePwritev` are stubs. |
| `verifier` | Sweep-line file verifier. `VerifyFile` with `Options` (ByteRange, LockMode, Log). |
| `internal/trace` | Structured zap logger setup for IO tracing (internal to verifyio). |
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
go build ./cmd/iotest-soak/
go build ./cmd/iotest-smoke/
go build ./cmd/iotest-verify/
go build ./cmd/iotest-dump/
go build ./cmd/iotest-posixbench/
```

Install to `$GOBIN` (or `~/go/bin` if `$GOBIN` is unset):

```sh
go install ./cmd/...
```

Run directly without a build step:

```sh
go run ./cmd/iotest-soak/ -path /tmp/soak -threads 8 -duration 30s
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
| `-blocksize` | `4096` | Total bytes per block on disk (use a power of two); must satisfy `block.BodyLen`'s body/stripe-CRC split — no header-size floor, since the header lives in the xattr, not the block |
| `-kind` | `decimal` | Body pattern: `decimal`, `prng`, `repeat`, `countup`, `zeros`, `ones` |
| `-iotrace` | `0` | IO trace level (0=off … 5=debug) |
| `-iologfile` | stderr | IO trace destination file |

---

### iotest-soak

Multi-threaded long-running stress test. Thread 0 operates exclusively on a
`.noOverlap` file; the remaining N−1 threads share a pool of ⌊(N−1)/2⌋
`.shared` files (clamped to at least one whenever there is a shared worker, so
`-threads 2` uses a single shared file). Each worker loops for the requested duration, randomly
picking a file from its pool, an operation (write or range-verify), and a
block-aligned offset, then executing it. OFD locking handles contention on
shared files. The run aborts immediately if body corruption (`CoverageMany`
or `Verdict != OK`) is detected.

```
iotest-soak -path /tmp/soak -threads 8 -duration 5m
iotest-soak -path /tmp/soak -threads 16 -duration 1h -blocksize 4096 -file-blocks 256
```

| Flag | Default | Description |
|------|---------|-------------|
| `-path` | `/tmp/iotest-soak` | Directory for soak data files (created if absent) |
| `-threads` | `4` | Number of concurrent worker goroutines |
| `-duration` | `30s` | How long to run (`0` = until anomaly) |
| `-blocksize` | `1024` | Bytes per block (use a power of two); must satisfy `block.BodyLen`'s body/stripe-CRC split |
| `-file-blocks` | `64` | Number of blocks per file (file size = blocksize × file-blocks) |
| `-iotrace` | `0` | IO trace level (0=off … 5=debug) |
| `-iologfile` | stderr | IO trace destination file |

File layout for `-threads 8`:

```
/tmp/soak/iotest-soak-0.noOverlap   ← thread 0 only
/tmp/soak/iotest-soak-0.shared      ← shared by threads 1–7
/tmp/soak/iotest-soak-1.shared
/tmp/soak/iotest-soak-2.shared
```

---

### iotest-verify

Verifies all blocks in a file written by `iotest-smoke` or `iotest-soak`.
By default prints only anomalous spans; `-verbose` prints every span.
Exits 1 if any anomaly is found.

```
iotest-verify -path /tmp/iotest-smoke.dat
iotest-verify -path /tmp/soak/iotest-soak-0.noOverlap -verbose
```

| Flag | Default | Description |
|------|---------|-------------|
| `-path` | (required) | Data file to verify |
| `-verbose` | false | Print all spans, not just anomalies |

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

## Design notes

**Intent-before-data**: the xattr is always written before the data block.
After a crash, a verifier may see an xattr with no matching data (recoverable)
but never data without a matching xattr.

**Fixed-size writes with `Put`**: `Writer.WriteBlock` uses `store.Put` rather
than `store.ReplaceRange`. With fixed block sizes at block-aligned offsets,
each new write to the same position uses the same xattr name
(`user.verifyio.<offset>-<blocksize>`) and `setxattr` replaces it in place — no
orphaned xattrs accumulate.

**Verifier races under concurrent writes**: `verifier.VerifyFile` snapshots the
set of xattr entries (offset/length) before acquiring per-span locks, but
**re-reads each header under the shared lock** before verifying. Because a writer
holds an exclusive lock across both the xattr write and the data write, a verifier
holding the shared lock always sees a mutually consistent header/body pair — so a
block reseeded after the snapshot is verified against its new header and passes,
rather than being falsely reported as `BODY_CORRUPT`. Two races remain benign and
are not anomalies: (1) a write completing entirely after the snapshot produces
apparent data with no xattr claim (`CoverageNone, AllZero=false`); (2) an entry
removed or replaced between the snapshot and the per-span lock is reported
`CoverageContended` (its header was gone when the lock was held). The soak test
only flags `CoverageMany` and `Verdict != VerdictOK` as real anomalies.
