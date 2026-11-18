# verifyio/bench — planned work

This file is planned work, not shipped behaviour. The `beegfs iotest` invocations in
the examples below describe the intended CLI surface; that command tree is not part of
this change, so nothing here can be run as written yet.

## mdtest: metadata benchmark with integrity verification

Implement `verifyio/bench/mdtest/` as a metadata-focused complement to `posixbench`.
Modelled on the mdtest benchmark but with per-file integrity verification via the
verifyio integrity layer (xattr-backed block writer/verifier).

### Phases

Run in sequence; each timed independently and reported as ops/sec:

1. **Create** — each worker creates N files, writing one verifiable block per file
   via `xattrstore.Writer` (data + xattr header in one operation)
2. **Stat** — `stat` each file; measures metadata lookup rate
3. **Read** — `open` + read block + `block.VerifyBlock` + `close`; read phase
   doubles as an integrity check (unlike plain mdtest)
4. **Remove** — `unlink` each file

### Directory modes

- **Unique dirs** (default) — each worker gets its own subdirectory, no contention
- **Shared dir** — all workers operate in one directory, stresses the metadata server

### File size

One block per file, small default (e.g. 4 KiB). Configurable — useful for measuring
the interaction between metadata rate and data volume.

### Open design questions

- **Phase selection** — run all four phases by default, with flags to skip individual
  ones (e.g. `--skip-remove` to leave files in place for a later `verify` pass)?
- **Tree structure** — mdtest supports branching factor and depth for deep directory
  trees; start flat for the first pass and add later?

### CLI shape (proposed)

    beegfs iotest mdtest run    -path /mnt/beegfs/testdir -files 1000 -threads 8
    beegfs iotest mdtest verify -path /mnt/beegfs/testdir

### Library shape (proposed)

    verifyio/bench/mdtest/
        config.go    — Config, Layout, phase flags
        runner.go    — per-phase runners, directory layout, Result type
        verifier.go  — post-run integrity re-check
