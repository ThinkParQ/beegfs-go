# verifyio — work in progress

Scope note: this file covers the `verifyio/` library and its standalone `iotest-*`
tools only. Anything about the `beegfs iotest` command tree — soak, inval, multi-node
fan-out — belongs in `ctl/internal/cmd/iotest/TODO.md` alongside the code it describes.

**That command tree is not part of this change.** `ctl/internal/cmd/iotest/` lands
separately, so every mention of `beegfs iotest` or of a path beneath it is
forward-looking — the paths do not resolve here and a reviewer cannot open them. Where
a claim below rests on something that lives with the CLI, it now says so outright
rather than leaving the reader to discover the file is absent.

## Background

The verifyio library was migrated into `beegfs-go` from a standalone project
and now lives at the top-level `verifyio/` package, consumed by the standalone
`iotest-*` tools and the `beegfs iotest` CLI.

### What's done

- `verifyio/{block,verifier,xattr,xattrstore,fileops}/` — xattr-backed block writer/verifier, range locking, sweep-line verifier
- `verifyio/bench/posixbench/` — sequential POSIX write/read benchmark with manifest
- `verifyio/trace/` — zap IO trace logger. Deliberately not under `internal/`: the CLI needs it too, and while it was internal that package could not import it
- `verifyio/cmd/` — CLI tools: iotest-smoke, iotest-verify, iotest-dump, iotest-posixbench, iotest-util
  (no standalone `iotest-soak`: it was deleted as a duplicate of the CLI's soak, which is a strict superset)
- All tools show usage+examples when invoked without arguments
- `iotest-verify` gives a clear error when a file has no xattr records (catches a mount without user xattrs enabled)
- All tools already participate in `go test ./...`, `go vet ./...`, staticcheck via `make test`
  (`check-linters` runs `go tool staticcheck ./...` and `go vet ./...`)
- No Makefile targets added — consistent with the `watch/cmd/test-*` precedent (dev tools use `go install` directly)
- `xattrstore.Writer` owns both halves of the store. `WriteBlock` writes through
  `ReplaceRange` (replace-and-shred, enforced rather than documented) and zeroes what
  shredding freed; `Truncate` drops the records covering a removed region before shortening
  the file. The `Store` primitives each maintain only one half, so reach for `Writer` unless
  there is no data file to keep in step
- `xattrstore.WriterConfig.Locking` fixes the lock policy for a Writer's lifetime rather than
  per call. A writer that locks only some of its operations offers no guarantee, since a
  reader need only land in one unlocked window; the zero value is rejected so the decision
  cannot be made by omission
- `block.AllVerdicts()` is the single source of truth for the verdict list, with a test that
  fails if a declared `Verdict` is missing from it. Two commands previously kept hand-written
  copies, and both silently missed `VerdictHeadTruncated` when it was added
- `xattrstore.CheckSafeToDestroy` guards every tool that clears a caller-supplied path, so a
  slip like `-path /etc/passwd` is refused rather than obeyed. Calling it is a convention
  here, not a gate: the `go/ast` test that fails when a writer resets a path without it
  lives with the CLI (`ctl/internal/cmd/iotest/smoke_guard_test.go`), so nothing in this
  package catches a new tool that omits the call. **As shipped here that test is absent
  entirely, so the guard is pinned by nothing** — the convention rests on review alone.
  Porting the test into `verifyio/` is the fix, and it is not done

---

## Open: a file-backed record store, to lift the capacity ceiling

**The largest known limitation of this library.** Storing one xattr per block caps how
much of a file it can describe at single-digit MiB. Measured with 4 KiB blocks: 2437
records / 9 MiB on tmpfs, 31 records / 124 KiB on ext4.

Two limits, and which binds depends on the filesystem:

- `setxattr` exhausts per-inode xattr space (ENOSPC). Filesystem-specific — ext4 has a
  single-block budget; tmpfs accepted 44,000+.
- `listxattr` cannot retrieve a name list larger than `XATTR_LIST_MAX` (65536 bytes,
  E2BIG). **A kernel limit in the VFS**, so identical on every filesystem including
  BeeGFS: a generous per-inode budget cannot be reached through it. On tmpfs it binds
  ~18× below what `setxattr` allowed.

This is not read-only. `Overlapping` lists the whole namespace on every call, so
`ReplaceRange` (hence `Writer.WriteBlock`), `Truncate`, `TryAcquireExclusive` and every
verifier sweep fail past the ceiling. A run that exceeds it does not degrade — it stops
partway through with `argument list too long` on a filesystem with terabytes free.

The plan is a `Store` implementation that writes records to a **separate data file**
instead of the inode's xattrs, keeping this package as a selectable mode (exercising the
filesystem's own xattr path is part of what verifyio tests). Preferred shape: spill over
— records up to the xattr ceiling stay in xattrs, the remainder goes to the sidecar, so a
large-file run still exercises xattrs in the same run.

Four things to settle first:

1. The ceiling is **not a constant** and cannot be hardcoded. 2437 is specific to 4 KiB
   blocks on tmpfs; the real budget is *name bytes*, which grow with offset magnitude.
   Track cumulative name length and spill before the next name would cross the limit.
2. The spill boundary is **not derivable from an offset**, so it is not a clean
   partition — two nodes writing different offsets each compute their own. A reader must
   consult both backends, and it must be defined which wins when a record exists in both.
3. The xattr half must stay **strictly** under the cap, not merely start under it: once
   the list crosses 64 KiB, reads of the spilled blocks fail too.
4. Metadata cost, not data cost, is where this concentrates: `Overlapping` already does a
   full `listxattr` plus a parse of every name per write — O(N) per write, O(N²) per run.
   The sidecar's index wants to be an in-memory map, not a file scan.

Rejected: sharding across many shadow files (only multiplies a small ceiling — 100 GiB at
4 KiB blocks would need ~11,000 of them); packing many headers into one xattr value (kills
the name-based extent query this package is built around, and forces read-modify-write per
block, losing per-attribute atomicity).

See `xattrstore`'s package doc for the measured numbers as a code-adjacent reference.
Related: `iotest-util xattr-capacity` reports the `setxattr` limit only, which is often
not the one that binds, so it should report both and their minimum.

---

## Open: `noneSpan` should take a shared range lock

`verifier.noneSpan` reports whether an uncovered span is all zero, and rechecks for a
record that appeared after the sweep's snapshot before calling non-zero bytes an anomaly.
Its recheck re-dispatches the **whole** span when it finds exactly one record, so a record
covering only part of the span makes the remainder — demonstrably non-zero — report as
covered. See the KNOWN LIMITATION comment on the function.

Unreachable against the tools in this package, which are single-threaded and use
`LockNone`. It becomes reachable alongside a concurrent writer.

**Do not fix it by splitting the span.** That was tried twice and failed twice, in
opposite directions: splitting inverted the scan-before-recheck ordering the function
depends on and produced a false anomaly on ~13% of sweeps run beside a writer, and adding
a re-confirmation on top of that reintroduced the false clean. The premise in the
function's own doc — "nothing to lock" — is the actual bug: writers lock *ranges*
(`lockRegion`, widened by `unionExtent`), not records, so the span is lockable even with
no record in it.

A shared range lock over the span, taken the way `oneSpan` takes one, measured 0% false
anomalies across every workload including the shred path, where rechecking still measured
~3% — `WriteBlock` removes records *before* it writes and zeroes, so during a shred the
bytes are genuinely unclaimed and no amount of re-reading the store distinguishes that
from corruption. Chunk the lock and scan per `noneSpanScanChunk` so a multi-GiB gap does
not stall writers.

**The lock only works against a `LockExclusive` writer.** These are advisory locks:
`Writer.takeLockIfNeeded` is a no-op under `LockNone`, so a verifier-side lock excludes nothing
there. That is consistent with doing this work alongside soak, the only writer that locks —
but it means the fix is not a general answer, and a `LockNone` writer racing a sweep stays
exposed by construction. The measurements above came from a soak-driven prototype and are
not reproducible from this branch, which has no locking writer.

Settle first: `verifyio/README.md`'s "Verifier races under concurrent writes" notes that
soak treats a concurrent-write `CoverageNone` as benign while the standalone verify tools
call it a hard FAIL. If the verify tools adopt soak's policy the recheck can be deleted
outright and no lock is needed. Decide the policy before writing the lock.

## Open: does BeeGFS mean to apply process semantics to `F_OFD_SETLK`?

**Discuss with the team**; this is a question for `beegfs-core`, not a `verifyio` change.

BeeGFS keys record locks by `(node, pid)` and ignores the open file description, even for
`F_OFD_SETLK` commands. Measured on the t91–t93 cluster 2026-08-17 (probe source:
`work/ofd-lock-probe/`): two fds in one process **both** get an OFD lock that XFS denies,
and either fd's unlock destroys the other's. Cross-node and cross-process enforcement are
correct.

That is OFD's defining guarantee not holding — the whole reason the API exists is that a
lock belongs to the description rather than the process. Whether it is intended, documented,
a client limitation, or simply unimplemented is unknown. `verifyio` no longer depends on the
answer (the OFD mode was removed rather than worked around), but anything else that reaches
for OFD locks on a BeeGFS mount will hit the same thing, silently.

## Open: abandoned-lock reporting for a long-running caller

`withTimeout` gives up on a syscall the kernel has not returned from, but Go cannot
interrupt it — the goroutine keeps running with an unknown eventual outcome, and the
`rangeLockTable` guard for that range is deliberately never freed, so the range becomes
permanently unacquirable. A registry of these (`AbandonedLocks()`) was removed in `b4a22f9`:
with any release error now fatal (`ReleaseErrorOrCause`), the first abandonment ends the
run, so nothing in this tree can accumulate a second entry for anyone to watch grow.

`ctl/internal/cmd/iotest`'s `soak` on the `iotest-ctl-workloads` branch still calls it, and
both uses are diagnostic rather than load-bearing: a poller that begins shutdown once three
syscalls are simultaneously abandoned, and the stuck-syscall list appended to the `LOCK
STATE STUCK` banner. Detection there does not depend on the registry — the failing worker's
own error path is what ends the run, as that watcher's own doc comment says. So the question
when ctl lands is whether the sharper report is worth an accumulator, not whether one is
needed to notice at all.

If it is, build it against what the soak actually needs rather than restoring the old shape.
The registry tracked the *syscall*, and its entry was deleted once the call finally returned
— whereas the thing that outlives the call, and the thing that actually makes the range
dead, is the leaked guard. A `Store`-level count of ranges lost to a timeout is closer to
the fact worth reporting.

## Open: the cross-process lock tests only reach a real mount by hand

`lock_crossprocess_linux_test.go` does two jobs. On tmpfs it checks our own wiring — that the
byte range we computed is the one `F_SETLK` receives, at each of the three `unix.Flock_t`
sites. Under `-path` against a BeeGFS mount the same tests characterize the DLM instead,
which is not a POSIX freebie here: see the `F_OFD_SETLK` section above.

Only the first runs in CI. The `-path` leg needs a mount and someone to remember the flag,
so it is manual and infrequent — `2577252`'s claim of a clean run against test-r9-03 sat
unrepeated for four days. Deliberate for now. A per-PR or nightly run belongs to a fuller
test suite that does not exist yet, not to this package.

Two things that suite should do and this file cannot: run these against a mount without
anyone opting in, and test *cross-node* enforcement — the child here is re-exec'd on the
same host, so cross-node remains unproven either way. That needs two invocations from two
hosts; `work/ofd-lock-probe/` is the shape that took.

## Open: refcounted intra-process shared locks

`rangeLockTable` conflicts on **any** overlap, shared-against-shared included, so two
goroutines on one `Store` cannot hold overlapping read locks — the loser gets `ErrLockBusy`
and the verifier reports `CoverageContended`. This is the one capability the removal of the
OFD mode gave up, accepted deliberately.

It is not a counter. `tryAcquire`'s own comment has the reason: holders need not share a
range, so releasing `[1024,3072)` punches a hole through the middle of a concurrently-held
`[0,4096)`, and a refcount per range does not see that. This needs a design — an interval
structure that tracks per-byte shared depth, or a release that re-asserts the surviving
holders' `F_RDLCK` — not a field.

Worth doing only if contention is measured to matter. Threads working disjoint ranges never
hit it, and two goroutines reading the same bytes gain nothing by overlapping. The related
risk — an all-contended sweep still printing PASS — is the coverage accounting's problem,
not this one.
