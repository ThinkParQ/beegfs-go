#!/usr/bin/env bash
#
# Exercises iotest-posixbench's run/verify subcommands: a clean run+verify
# round trip, a sweep of -kind/-layout/-threads/-read flag
# combinations, byte-level corruption and truncation that verify must flag
# (with the right cause, not a generic label), and a misdirected-write check
# in the default (decimal) mode, which the low-bit-only kinds could not detect.
#
# Not part of `make test` -- these drive the built binary against real files,
# which a Go unit test in verifyio/bench/posixbench/*_test.go doesn't do. Run
# by hand after touching verifyio/cmd/iotest-posixbench or
# verifyio/bench/posixbench.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: exercise-posixbench.sh <scratch-dir>

Builds iotest-posixbench and runs it through a run/verify round trip, a
-kind/-layout/-read flag sweep, byte-level corruption, truncation,
and a misdirected-write check -- against real files under <scratch-dir>.

  <scratch-dir>  Directory to write test files into (created if missing). No
                 xattr support is needed -- posixbench never calls
                 setxattr/getxattr -- so this also runs on filesystems (or
                 mount options) verifyio's other tools can't use. Its
                 contents are left in place on exit for inspection; this
                 script never deletes a directory you gave it.

Environment:
  IOTEST_BINDIR  If set, use the iotest-posixbench binary already present in
                 this directory instead of building it from source -- for a
                 build machine that differs from the client you want to test
                 against. Build with -o: `go build ./cmd/...` (or bare `cmd`,
                 which is a reserved Go pattern meaning something else
                 entirely) builds nothing to disk.

                 If the repo is on a mount shared between the build machine
                 and the client (e.g. NFS), build straight onto it -- no copy
                 needed, and the client can run this same script in place:

                   # on the build node
                   mkdir -p /data/Code/beegfs/iotest-bins
                   go build -o /data/Code/beegfs/iotest-bins/iotest-posixbench ./verifyio/cmd/iotest-posixbench

                   # on the client
                   IOTEST_BINDIR=/data/Code/beegfs/iotest-bins \
                     /data/Code/beegfs/beegfs-go/verifyio/scripts/exercise-posixbench.sh /mnt/beegfs/test

                 Otherwise, copy the binary (and this script) over:

                   # on the build node
                   mkdir -p /tmp/iotest-bins
                   go build -o /tmp/iotest-bins/iotest-posixbench ./verifyio/cmd/iotest-posixbench
                   scp -r /tmp/iotest-bins verifyio/scripts/exercise-posixbench.sh client:

                   # on the client
                   IOTEST_BINDIR=/tmp/iotest-bins ./exercise-posixbench.sh /mnt/beegfs/test
EOF
}

if [ "$#" -eq 0 ]; then
  usage
  exit 2
fi
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  usage
  exit 0
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKDIR="$1"
mkdir -p "$WORKDIR"
OUTFILE="$(mktemp -t posixbench-exercise-out.XXXXXX)"
BUILT_BINDIR=""

cleanup() {
  rm -f "$OUTFILE"
  # Only remove the bin dir if we created it ourselves via mktemp below --
  # IOTEST_BINDIR is caller-owned, like WORKDIR, and is never removed here.
  if [ -n "$BUILT_BINDIR" ]; then
    rm -rf "$BUILT_BINDIR"
  fi
}
trap cleanup EXIT

pass=0
fail=0

section() { printf '\n=== %s ===\n' "$1"; }

check() {
  local desc="$1" expected="$2"; shift 2
  printf -- '--- %s\n    $ %s\n' "$desc" "$*"
  set +e
  "$@" >"$OUTFILE" 2>&1
  local rc=$?
  set -e
  sed 's/^/    /' "$OUTFILE"
  if [ "$rc" -eq "$expected" ]; then
    printf '    OK (exit %d)\n' "$rc"
    pass=$((pass + 1))
  else
    printf '    FAIL: exit %d, want %d\n' "$rc" "$expected"
    fail=$((fail + 1))
  fi
}

check_contains() {
  local desc="$1" expected="$2" needle="$3"; shift 3
  printf -- '--- %s\n    $ %s\n' "$desc" "$*"
  set +e
  "$@" >"$OUTFILE" 2>&1
  local rc=$?
  set -e
  sed 's/^/    /' "$OUTFILE"
  if [ "$rc" -ne "$expected" ]; then
    printf '    FAIL: exit %d, want %d\n' "$rc" "$expected"
    fail=$((fail + 1))
  elif ! grep -qF -- "$needle" "$OUTFILE"; then
    printf '    FAIL: output does not contain %q\n' "$needle"
    fail=$((fail + 1))
  else
    printf '    OK (exit %d, found %q)\n' "$rc" "$needle"
    pass=$((pass + 1))
  fi
}

if [ -n "${IOTEST_BINDIR:-}" ]; then
  section "Using prebuilt binary from IOTEST_BINDIR"
  BINDIR="$IOTEST_BINDIR"
  if [ ! -x "$BINDIR/iotest-posixbench" ]; then
    echo "FATAL: IOTEST_BINDIR=$BINDIR is missing an executable iotest-posixbench" >&2
    exit 1
  fi
else
  section "Build"
  BUILT_BINDIR="$(mktemp -d -t iotest-bin.XXXXXX)"
  BINDIR="$BUILT_BINDIR"
  ( cd "$REPO_ROOT" && go build -o "$BINDIR/iotest-posixbench" ./verifyio/cmd/iotest-posixbench )
fi
PB="$BINDIR/iotest-posixbench"
echo "using binary from $BINDIR"

# -hostname defaults to this node's name and -run to a UTC timestamp; both are
# woven into every data filename. The sections below that corrupt a file BY NAME
# pin both to known values. Sections that do not name a file leave them
# defaulted, so the default path is exercised too.
PBHOST=exercise
PBRUN=fixedrun
FIX="-hostname $PBHOST -run $PBRUN"          # for run
FIXV="-hostname $PBHOST -run $PBRUN"         # for verify
PFX="pbench-$PBRUN-$PBHOST"
# The results file's stem, in one place. It is deliberately not a "posixbench-"
# name (see resultsStem in Go); hardcoding it at a dozen sites here is how the
# script and the code would drift the next time either moves.
PBRES="pbresults"

section "Clean round trip: run -> verify"
d1="$WORKDIR/clean"
check_contains "run: default kind (decimal), 1-to-1, 2 threads" 0 "Manifest:" \
  "$PB" run -path "$d1" -threads 2 -block-size 4096 -file-size 65536
check_contains "verify: clean run passes" 0 "PASS" \
  "$PB" verify -path "$d1"

section "Multi-node: two hosts sharing one directory must not collide"
# -hostname is woven into both the data filenames and the manifest name, which
# is what lets several nodes benchmark one directory at once. Before it was
# wired to `run`, two concurrent runs silently overwrote each other's blocks and
# verify was a coin flip -- measured 6 PASS / 6 FAIL over 12 trials, with the
# failures looking exactly like filesystem corruption.
dmn="$WORKDIR/multinode"
"$PB" run -path "$dmn" -hostname node-a -run r1 -threads 1 -block-size 4096 -file-size 16384 >/dev/null
"$PB" run -path "$dmn" -hostname node-b -run r1 -threads 1 -block-size 4096 -file-size 16384 >/dev/null
check_contains "verify: node-a's data is intact alongside node-b's" 0 "PASS" \
  "$PB" verify -path "$dmn" -hostname node-a
check_contains "verify: node-b's data is intact alongside node-a's" 0 "PASS" \
  "$PB" verify -path "$dmn" -hostname node-b
# The property behind it: separate data files AND separate manifests.
check "each host got its own data file and manifest" 0 \
  test -f "$dmn/pbench-r1-node-a-w000-f000.dat" -a -f "$dmn/pbench-r1-node-b-w000-f000.dat" \
       -a -f "$dmn/posixbench-r1-node-a.json" -a -f "$dmn/posixbench-r1-node-b.json"

section "Run stamps: two runs from one host in one directory"
# -run defaults to a UTC timestamp, so repeated runs into one directory do not
# overwrite each other. Verify must then refuse to guess which one is meant.
drr="$WORKDIR/reruns"
"$PB" run -path "$drr" -hostname h -run runA -threads 1 -block-size 4096 -file-size 16384 >/dev/null
"$PB" run -path "$drr" -hostname h -run runB -threads 1 -block-size 4096 -file-size 16384 >/dev/null
check "both runs kept their own data" 0 \
  test -f "$drr/pbench-runA-h-w000-f000.dat" -a -f "$drr/pbench-runB-h-w000-f000.dat"
check_contains "verify: refuses to guess between two runs (usage, not FAIL)" 2 "holds 2 runs" \
  "$PB" verify -path "$drr"
check_contains "verify -run: names one and passes" 0 "PASS" \
  "$PB" verify -path "$drr" -run runA

section "Results file: a completed run leaves a self-contained record"
dres="$WORKDIR/results"
"$PB" run -path "$dres" -hostname h -run rec -threads 2 -block-size 4096 -file-size 16384 -read >/dev/null
check "a results file was written beside the manifest" 0 \
  test -f "$dres/$PBRES-rec-h.json"
# Self-contained is the point: the record is meant to be copied into an archive
# without its data or manifest, so it must carry the config and the numbers.
check_contains "results: embeds the manifest it was produced from" 0 '"runID": "rec"' \
  cat "$dres/$PBRES-rec-h.json"
check_contains "results: carries the write throughput" 0 '"mbPerSecond"' \
  cat "$dres/$PBRES-rec-h.json"
check_contains "results: records the read phase when one ran" 0 '"read"' \
  cat "$dres/$PBRES-rec-h.json"
check_contains "results: environment defaults to unknown, not blank" 0 '"beegfsVersion": "unknown"' \
  cat "$dres/$PBRES-rec-h.json"

section "Flag variety: -kind sweep"
for kind in decimal prng zeros ones countup repeat; do
  dk="$WORKDIR/kind-$kind"
  check_contains "run -kind $kind" 0 "Manifest:" \
    "$PB" run -path "$dk" -threads 1 -block-size 4096 -file-size 32768 -kind "$kind"
  check_contains "verify -kind $kind" 0 "PASS" \
    "$PB" verify -path "$dk"
done

section "Flag variety: -layout n-to-1 (workers share one file)"
dn="$WORKDIR/nto1"
check_contains "run -layout n-to-1" 0 "Manifest:" \
  "$PB" run -path "$dn" -threads 3 -block-size 4096 -file-size 32768 -layout n-to-1
check_contains "verify -layout n-to-1" 0 "PASS" \
  "$PB" verify -path "$dn"

section "Flag variety: -read (write + sequential read-back bandwidth)"
dr="$WORKDIR/withread"
check_contains "run -read" 0 "Read:" \
  "$PB" run -path "$dr" -threads 2 -block-size 4096 -file-size 65536 -read
check_contains "verify after -read" 0 "PASS" \
  "$PB" verify -path "$dr"

section "Byte-level corruption: verify must report a data mismatch, not a generic label"
dc="$WORKDIR/corrupt"
"$PB" run -path "$dc" $FIX -threads 1 -block-size 4096 -file-size 16384 >/dev/null
printf '\xff\xff\xff\xff' | dd of="$dc/$PFX-w000-f000.dat" bs=1 seek=100 count=4 conv=notrunc status=none
check_contains "verify: flags the corruption" 1 "FAIL" \
  "$PB" verify -path "$dc" $FIXV -verbose
check_contains "verify -verbose: names the cause (data mismatch, not truncation)" 1 "data mismatch" \
  "$PB" verify -path "$dc" $FIXV -verbose

section "Truncation: verify must report a short read, not a generic mismatch label"
dt="$WORKDIR/truncated"
"$PB" run -path "$dt" $FIX -threads 1 -block-size 4096 -file-size 16384 >/dev/null
truncate -s 6000 "$dt/$PFX-w000-f000.dat" # cuts block 1 short, drops block 2 and 3 entirely
check_contains "verify: flags the truncation" 1 "FAIL" \
  "$PB" verify -path "$dt" $FIXV -verbose
check_contains "verify -verbose: names the cause (short read, not corruption)" 1 \
  "short read (truncated/missing block)" \
  "$PB" verify -path "$dt" $FIXV -verbose

section "Misdirected write, default (decimal) kind: must be detected"
dm="$WORKDIR/misdirected"
# -seed is pinned, not left to EnsureSeed's random value, because decimal
# detection is probabilistic: each block has ~1/512 chance of the two files'
# seeds reducing to the same start (see block.Kind). Measured over 2,000,000
# random seeds, at least one of these 4 blocks collides in 0.77% of runs, which
# reports 3 anomalies and fails the assertion below. This was deterministic
# under the old XOR-fold and is not any more. This seed is known to detect all
# four; a run that fails here is a real regression, not a coincidence.
check_contains "run: 2 workers, default kind" 0 "Manifest:" \
  "$PB" run -path "$dm" $FIX -threads 2 -block-size 4096 -file-size 16384 -seed 123456789
# Overwrite worker 1's entire file with worker 0's: detectable only because
# GenerateBody mixes the whole seed, not just its low bits.
cp "$dm/$PFX-w000-f000.dat" "$dm/$PFX-w001-f000.dat"
# Anchored on the printed prefix, two spaces and all: a bare "4 anomaly(s)"
# is a substring of "14 anomaly(s)" and "24 anomaly(s)", so the exact count
# this check exists to pin would not actually be pinned.
check_contains "verify: flags every block in the misdirected file" 1 \
  "FAIL  4 anomaly(s)" \
  "$PB" verify -path "$dm" $FIXV

section "Access pattern: random visit order must still verify"
# Block CONTENT is keyed on block index, not on write order, so a randomly
# ordered write verifies identically to a sequential one. That is the property
# that makes new access patterns cheap; pin it.
dpat="$WORKDIR/pattern"
check_contains "run -pattern random" 0 "Results:" \
  "$PB" run -path "$dpat" -run p -hostname h -pattern random -threads 2 -block-size 4096 -file-size 32768
check_contains "verify: a randomly written file is still clean" 0 "PASS" \
  "$PB" verify -path "$dpat" -run p
check_contains "run: refuses an unknown pattern by name (usage, not FAIL)" 2 "unknown pattern" \
  "$PB" run -path "$WORKDIR/badpat" -pattern bogus
# The record must say HOW it was written, or two runs are not comparable.
check_contains "results: records the pattern used" 0 '"pattern": "random"' \
  cat "$dpat/$PBRES-p-h.json"
check_contains "results: records the io type used" 0 '"ioType": "buffered"' \
  cat "$dpat/$PBRES-p-h.json"
check_contains "results: carries per-operation latency" 0 '"latencyMs"' \
  cat "$dpat/$PBRES-p-h.json"

section "Environment capture: a record must say what it ran against"
# A bandwidth number is not comparable to one from a year ago unless the record
# says what the client was configured like. -env-file reads any key=value file
# -- on BeeGFS, /proc/fs/beegfs/<client>/config, which is the EFFECTIVE runtime
# config rather than the file on disk. Stored as key/value so an A-vs-B
# comparison is a field diff rather than a text diff.
denv="$WORKDIR/envcapture"
mkdir -p "$denv"
printf '# banner\ntuneFileCacheType = buffered\nconnMaxInternodeNum = 12\nnot a kv line\n' \
  > "$denv/client.conf"
check_contains "run: accepts -env-file and -externaldata" 0 "Results:" \
  "$PB" run -path "$denv/w" -run e -hostname h -threads 1 -block-size 4096 -file-size 16384 \
    -env-file "$denv/client.conf" -externaldata "exercise-script"
check_contains "results: records a captured setting" 0 '"tuneFileCacheType": "buffered"' \
  cat "$denv/w/$PBRES-e-h.json"
check_contains "results: records the free-form context" 0 '"external": "exercise-script"' \
  cat "$denv/w/$PBRES-e-h.json"
# Unfilled fields must read "unknown" rather than being absent or blank, so a
# reader can tell "not captured" from "not applicable".
check_contains "results: uncaptured environment stays explicit" 0 '"beegfsVersion": "unknown"' \
  cat "$denv/w/$PBRES-e-h.json"
# A named file that cannot be read must stop the run: a record silently missing
# the context it was meant to be compared by is worse than no record.
check "an unreadable -env-file is a usage error, not a silent skip" 2 \
  "$PB" run -path "$denv/x" -env-file "$denv/absent.conf" -threads 1 -block-size 4096 -file-size 16384

section "Exit codes: one code per outcome, not one for everything"
# climain.Die hardcodes exit 1, which this tool reserves for FAIL. Before these
# checks, a manifest the tool could not read and a file full of corruption
# exited identically, an interrupted run exited 0, and an ambiguous -run was
# reported as corrupt data. A wrapper branching on the code got all three wrong.
dxc="$WORKDIR/exitcodes"
"$PB" run -path "$dxc" -run a -hostname h -threads 1 -block-size 4096 -file-size 16384 >/dev/null
check "0: verify on clean data"        0 "$PB" verify -path "$dxc"
check "2: an unknown -kind is usage"   2 "$PB" run -path "$WORKDIR/xk" -kind bogus
check "2: an unknown -pattern is usage" 2 "$PB" run -path "$WORKDIR/xp" -pattern bogus
check "4: an empty directory is NO_DATA, not FAIL" 4 \
  "$PB" verify -path "$WORKDIR/emptydir"
# Two runs in one directory: the operator must narrow it. That is a command-line
# problem, not corrupt data.
"$PB" run -path "$dxc" -run b -hostname h -threads 1 -block-size 4096 -file-size 16384 >/dev/null
check "2: an ambiguous -run is usage, not FAIL" 2 "$PB" verify -path "$dxc"
# A manifest that exists but cannot be parsed is an ERROR, and must not be
# reported as "no manifest here" -- that sends an operator hunting a file that
# is sitting right in front of them.
dxb="$WORKDIR/badmanifest"
"$PB" run -path "$dxb" -run a -hostname h -threads 1 -block-size 4096 -file-size 16384 >/dev/null
truncate -s 40 "$dxb/posixbench-a-h.json"
check "5: a corrupt manifest is ERROR" 5 "$PB" verify -path "$dxb"
check_contains "5: and it names the file rather than claiming none exists" 5 "parse" \
  "$PB" verify -path "$dxb"
# An interrupted run leaves a manifest with no finishedAt. Verifying it must not
# report the blocks the run never reached as corrupt data: that is exit 1, "the
# data is wrong", over a filesystem that did nothing wrong. It is exit 3.
#
# The unstamped manifest is produced by renaming the key rather than by timing a
# signal, so the check is deterministic on any filesystem: an unknown key is
# ignored by the parser, which leaves exactly the state an interrupt produces,
# and the document stays valid JSON (finishedAt is last, so deleting the line
# would strand a comma).
dxi="$WORKDIR/incomplete"
"$PB" run -path "$dxi" -run a -hostname h -threads 1 -block-size 4096 -file-size 16384 >/dev/null
check "0: control -- the same run verifies clean while it is stamped" 0 "$PB" verify -path "$dxi"
sed -i 's/"finishedAt"/"xfinishedAt"/' "$dxi/posixbench-a-h.json"
printf '\xff\xff\xff\xff' | dd of="$dxi/pbench-a-h-w000-f000.dat" bs=1 seek=100 count=4 conv=notrunc status=none
check "3: anomalies over an unfinished run are INCOMPLETE, not FAIL" 3 \
  "$PB" verify -path "$dxi"
check_contains "3: and it says the run did not finish" 3 "did not finish" \
  "$PB" verify -path "$dxi"

# A data file that is gone is data loss, so it is FAIL -- not ERROR, which says
# the tool could not do its job and reads to a wrapper as a setup problem. And
# the sweep must carry on past it: it used to abort at the first unopenable file,
# so corruption in the files AFTER it was never reported at all.
dxm="$WORKDIR/missingfile"
"$PB" run -path "$dxm" -run a -hostname h -threads 3 -block-size 4096 -file-size 16384 >/dev/null
rm "$dxm/pbench-a-h-w000-f000.dat"
printf '\xff\xff\xff\xff' | dd of="$dxm/pbench-a-h-w002-f000.dat" bs=1 seek=100 count=4 conv=notrunc status=none
check "1: a missing data file is FAIL, not ERROR" 1 "$PB" verify -path "$dxm"
check_contains "1: and it names the file as missing" 1 "missing data file" \
  "$PB" verify -path "$dxm" -verbose
# 1 absent file + 1 corrupt block in a LATER file. If the sweep still aborted at
# the missing one, the corrupt block would never be counted and this would be 1.
# Anchored: "2 anomaly(s)" alone also matches "12 anomaly(s)", measured against
# a 12-block run with every block corrupted.
check_contains "1: and the sweep goes on to the files after it" 1 "FAIL  2 anomaly(s)" \
  "$PB" verify -path "$dxm"

# What the sweep covered, printed on every verdict. Without it a PASS carries no
# volume: an operator cannot tell a three-file sweep from a three-hundred-file
# one, and a sweep that quietly shrank looks exactly like a clean one. The
# numbers are checked against a known run shape, not merely for presence.
dcov="$WORKDIR/coverage"
"$PB" run -path "$dcov" -run a -hostname h -threads 3 -files-per-worker 2 \
  -block-size 4096 -file-size 16384 >/dev/null
# The byte figure is part of the needle: it is derived as blocks * blockSize,
# and dropping the multiplier prints "24 B" for the same 96 KiB sweep with
# every other gate green.
#
# This also doubles as the end-to-end round trip for -files-per-worker, which
# nothing else exercises: a manifest that lost the field would sweep 3 regions
# rather than 6, and the independent ExpectedBlocks would then disagree and
# turn this into SHORT at exit 5.
check_contains "0: verify says how many regions, blocks and bytes it compared" 0 \
  "Verified: 6 region(s), 24 block(s), 96.00 KiB" "$PB" verify -path "$dcov"

# A run whose -run begins with "results" used to be permanently unverifiable.
# The manifest name fell inside the results-file namespace, the listing filtered
# it out by prefix, and the tool reported "no posixbench manifest in <dir>" at
# exit 4 -- "nothing to verify" -- over a manifest and a full set of data files
# sitting in that directory. An explicit -run could not recover it.
dcol="$WORKDIR/collide"
"$PB" run -path "$dcol" -run results1 -hostname h -threads 1 -block-size 4096 -file-size 16384 >/dev/null
check "0: a -run beginning 'results' is still verifiable" 0 "$PB" verify -path "$dcol"
check "0: and with an explicit -run too" 0 "$PB" verify -path "$dcol" -run results1
# Every JSON object unmarshals into a Manifest with all fields zeroed, so a
# stray file in the tool's own namespace used to list as a run with an empty
# runID -- which made a real run beside it "ambiguous" and refused both.
printf '{}\n' > "$dcol/posixbench-stray.json"
check "5: a stray JSON file in the namespace is reported, not listed as a run" 5 \
  "$PB" verify -path "$dcol" -run results1
check_contains "5: and it says which file and why" 5 "not a version" \
  "$PB" verify -path "$dcol" -run results1

# FAIL stays 1, so the one code that carries a verdict still means what it did.
dxf="$WORKDIR/failing"
"$PB" run -path "$dxf" -run a -hostname h -threads 1 -block-size 4096 -file-size 16384 >/dev/null
printf '\xff\xff\xff\xff' | dd of="$dxf/pbench-a-h-w000-f000.dat" bs=1 seek=100 count=4 conv=notrunc status=none
check "1: corrupted data is still FAIL" 1 "$PB" verify -path "$dxf"

section "Summary"
printf '%d passed, %d failed\n' "$pass" "$fail"
if [ "$fail" -ne 0 ]; then
  exit 1
fi
