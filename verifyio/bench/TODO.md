# verifyio/bench — planned work

## posixbench: ECC/no-xattr bit-rot regression test

Add a test (mirroring `TestVerifierDetectsCorruption`, but with `NoXattr: true`) that
runs a `--no-xattr` benchmark, flips a byte on disk in a data file, and asserts
`verifyECC` reports a per-stripe `CRC mismatch` anomaly. The bit-rot-detection capability
works today (the in-file CRC32C stripes), but only the default full-body path is covered by
a test; this locks in the ECC path against regression. Note `--no-xattr` filenames embed the
block size (`pbench-bs<N>-w000-f000.dat`). Verification stays CRC-only by design (no-xattr is
the throughput-benchmark mode; full content verification is the default xattr mode's job).

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
