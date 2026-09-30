#!/usr/bin/env bash
#
# Exercises iotest-smoke, iotest-verify, iotest-dump, and iotest-util against
# real files and real xattrs: a clean write/verify/dump round trip, a sweep of
# -kind variants, byte-level and xattr-level corruption that verify/dump must
# flag, and iotest-util's own write/xattr-capacity operations.
#
# Not part of `make test` -- these drive the built binaries against a real
# filesystem path, which a Go unit test in verifyio/*_test.go doesn't do. Run
# by hand after touching verifyio/cmd/iotest-{smoke,verify,dump,util} or the
# packages they call into.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: exercise-smoke-verify-dump.sh <scratch-dir>

Builds iotest-smoke/iotest-verify/iotest-dump/iotest-util and runs them
through a write/verify/dump round trip, a -kind/-blocksize flag sweep, and
byte-level/xattr-level corruption that verify and dump must flag -- against
real files under <scratch-dir>.

  <scratch-dir>  Directory to write test files into (created if missing).
                 Must support user xattrs -- point it at a BeeGFS mount to
                 exercise this against BeeGFS specifically instead of local
                 disk/tmpfs. BeeGFS needs sysXAttrsEnabled (beegfs-client.conf)
                 and storeClientXAttrs (beegfs-meta.conf); the live client
                 value is in /proc/fs/beegfs/<mount-id>/config.
                 Its contents are left in place on exit for inspection; this
                 script never deletes a directory you gave it.

Environment:
  IOTEST_BINDIR  If set, use the iotest-smoke/-verify/-dump/-util binaries
                 already present in this directory instead of building them
                 from source -- for a build machine that differs from the
                 client you want to test against. Build one binary at a time
                 with -o: `go build ./cmd/...` (or bare `cmd`, which is a
                 reserved Go pattern meaning something else entirely) builds
                 nothing to disk, since a multi-main-package build with no -o
                 compiles as a check and writes no output.

                 If the repo is on a mount shared between the build machine
                 and the client (e.g. NFS), build straight onto it -- no copy
                 needed, and the client can run this same script in place:

                   # on the build node
                   mkdir -p /data/Code/beegfs/iotest-bins
                   for t in iotest-smoke iotest-verify iotest-dump iotest-util; do
                     go build -o /data/Code/beegfs/iotest-bins/$t ./verifyio/cmd/$t
                   done

                   # on the client
                   IOTEST_BINDIR=/data/Code/beegfs/iotest-bins \
                     /data/Code/beegfs/beegfs-go/verifyio/scripts/exercise-smoke-verify-dump.sh /mnt/beegfs/test

                 Otherwise, copy the binaries (and this script) over:

                   # on the build node
                   mkdir -p /tmp/iotest-bins
                   for t in iotest-smoke iotest-verify iotest-dump iotest-util; do
                     go build -o /tmp/iotest-bins/$t ./verifyio/cmd/$t
                   done
                   scp -r /tmp/iotest-bins verifyio/scripts/exercise-smoke-verify-dump.sh client:

                   # on the client
                   IOTEST_BINDIR=/tmp/iotest-bins ./exercise-smoke-verify-dump.sh /mnt/beegfs/test
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
OUTFILE="$(mktemp -t iotest-exercise-out.XXXXXX)"
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

# check DESC EXPECTED_EXIT -- CMD...   runs CMD, asserts its exit code.
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

# check_contains DESC EXPECTED_EXIT NEEDLE -- CMD...   as check, plus asserts
# NEEDLE appears somewhere in combined stdout+stderr.
# Like check_contains, but the needle is an extended regex. For a command whose
# output legitimately takes one of several forms, so that no fixed string can
# assert the outcome without also matching a case that says nothing.
check_matches() {
  local desc="$1" expected="$2" re="$3"; shift 3
  printf -- '--- %s\n    $ %s\n' "$desc" "$*"
  set +e
  "$@" >"$OUTFILE" 2>&1
  local rc=$?
  set -e
  sed 's/^/    /' "$OUTFILE"
  if [ "$rc" -ne "$expected" ]; then
    printf '    FAIL: exit %d, want %d\n' "$rc" "$expected"
    fail=$((fail + 1))
  elif ! grep -qE -- "$re" "$OUTFILE"; then
    printf '    FAIL: output does not match %q\n' "$re"
    fail=$((fail + 1))
  else
    printf '    OK (exit %d, matched %q)\n' "$rc" "$re"
    pass=$((pass + 1))
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

# check_lacks DESC EXPECTED_EXIT NEEDLE -- CMD...   as check, plus asserts
# NEEDLE does NOT appear in combined stdout+stderr.
#
# For a misdiagnosis: some fixes are only observable as the absence of the
# wrong answer, because the right answer was already being printed for an
# unrelated reason. Asserting the presence of the right answer passes in both
# cases and pins nothing.
check_lacks() {
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
  elif grep -qF -- "$needle" "$OUTFILE"; then
    printf '    FAIL: output contains %q, which it must not\n' "$needle"
    fail=$((fail + 1))
  else
    printf '    OK (exit %d, no %q)\n' "$rc" "$needle"
    pass=$((pass + 1))
  fi
}

# check_lacks_matching DESC EXPECTED_EXIT REGEX -- CMD...   as check_lacks, but
# the needle is an extended regex, so it can be anchored.
#
# Needed for the forged-line assertions: the property is that no LINE BEGINS
# with a given string, which no fixed-string search can express. A multi-line
# fixed needle does not express it either -- grep -F splits on newlines and
# treats the parts as alternatives, so a leading newline contributes an empty
# pattern that matches every line.
check_lacks_matching() {
  local desc="$1" expected="$2" re="$3"; shift 3
  printf -- '--- %s\n    $ %s\n' "$desc" "$*"
  set +e
  "$@" >"$OUTFILE" 2>&1
  local rc=$?
  set -e
  sed 's/^/    /' "$OUTFILE"
  if [ "$rc" -ne "$expected" ]; then
    printf '    FAIL: exit %d, want %d\n' "$rc" "$expected"
    fail=$((fail + 1))
  elif grep -qE -- "$re" "$OUTFILE"; then
    printf '    FAIL: output has a line matching %q, which it must not\n' "$re"
    fail=$((fail + 1))
  else
    printf '    OK (exit %d, no line matching %q)\n' "$rc" "$re"
    pass=$((pass + 1))
  fi
}

if [ -n "${IOTEST_BINDIR:-}" ]; then
  section "Using prebuilt binaries from IOTEST_BINDIR"
  BINDIR="$IOTEST_BINDIR"
  for t in iotest-smoke iotest-verify iotest-dump iotest-util; do
    if [ ! -x "$BINDIR/$t" ]; then
      echo "FATAL: IOTEST_BINDIR=$BINDIR is missing an executable $t" >&2
      exit 1
    fi
  done
else
  section "Build"
  BUILT_BINDIR="$(mktemp -d -t iotest-bin.XXXXXX)"
  BINDIR="$BUILT_BINDIR"
  ( cd "$REPO_ROOT" && go build -o "$BINDIR/iotest-smoke" ./verifyio/cmd/iotest-smoke )
  ( cd "$REPO_ROOT" && go build -o "$BINDIR/iotest-verify" ./verifyio/cmd/iotest-verify )
  ( cd "$REPO_ROOT" && go build -o "$BINDIR/iotest-dump" ./verifyio/cmd/iotest-dump )
  ( cd "$REPO_ROOT" && go build -o "$BINDIR/iotest-util" ./verifyio/cmd/iotest-util )
fi
SMOKE="$BINDIR/iotest-smoke"
VERIFY="$BINDIR/iotest-verify"
DUMP="$BINDIR/iotest-dump"
UTIL="$BINDIR/iotest-util"
echo "using binaries from $BINDIR"

# Confirm the scratch dir actually supports user xattrs before relying on it --
# iotest-verify already gives a clear error for this case, but failing fast
# here with a pointed message saves working through every check below only to
# hit the same root cause repeatedly.
if ! setfattr -n user.iotest_exercise_probe -v x "$WORKDIR" 2>/dev/null && \
   ! (touch "$WORKDIR/.xattr_probe" && setfattr -n user.iotest_exercise_probe -v x "$WORKDIR/.xattr_probe"); then
  echo "FATAL: $WORKDIR does not appear to support user xattrs (needed by every check below)" >&2
  exit 1
fi
rm -f "$WORKDIR/.xattr_probe"

section "Clean round trip: smoke -> verify -> dump"
f1="$WORKDIR/smoke.dat"
check_matches "smoke: write 8 blocks, 1024B, default kind (decimal)" 0 'OK: +8$' \
  "$SMOKE" -path "$f1" -blocks 8 -blocksize 1024
check_contains "verify: clean file passes" 0 "PASS" \
  "$VERIFY" -path "$f1"
check_contains "dump: reports all 8 blocks" 0 "8 block(s)" \
  "$DUMP" -path "$f1"
check_contains "verify -verbose: prints every span, not just anomalies" 0 "coverage=one" \
  "$VERIFY" -path "$f1" -verbose

section "Flag variety: -kind sweep (smoke -> verify -> dump for each)"
for kind in decimal prng repeat countup zeros ones; do
  fk="$WORKDIR/smoke-$kind.dat"
  check_matches "smoke -kind $kind" 0 'OK: +4$' \
    "$SMOKE" -path "$fk" -blocks 4 -blocksize 2048 -kind "$kind"
  check_contains "verify -kind $kind: clean file passes" 0 "PASS" \
    "$VERIFY" -path "$fk"
  check_contains "dump -kind $kind: reports the kind used" 0 "kind=$kind" \
    "$DUMP" -path "$fk"
done

section "Flag variety: -blocksize sweep (516 always works; large sizes too)"
for bs in 516 4096 66048; do
  fb="$WORKDIR/smoke-bs$bs.dat"
  check_matches "smoke -blocksize $bs" 0 'OK: +3$' \
    "$SMOKE" -path "$fb" -blocks 3 -blocksize "$bs"
  check_contains "verify -blocksize $bs: clean file passes" 0 "PASS" \
    "$VERIFY" -path "$fb"
done

section "Byte-level corruption: verify and dump must flag it"
fc="$WORKDIR/corrupt-byte.dat"
"$SMOKE" -path "$fc" -blocks 4 -blocksize 1024 -kind decimal >/dev/null
# Flip a byte inside block 1's body (not its header). KindDecimal's body is
# always in ['0'-'9', ' '], so 0xFF is unconditionally a mismatch.
printf '\xff' | dd of="$fc" bs=1 seek=1100 count=1 conv=notrunc status=none
check_contains "verify: flags the corrupted block" 1 "FAIL" \
  "$VERIFY" -path "$fc"
check_contains "dump: reports a non-OK verdict for the corrupted block" 0 "BODY_CRC_MISMATCH" \
  "$DUMP" -path "$fc"

section "Truncation: verify and dump must flag a short file, not silently pass"
ft="$WORKDIR/truncated.dat"
"$SMOKE" -path "$ft" -blocks 4 -blocksize 1024 -kind decimal >/dev/null
truncate -s 3000 "$ft" # cuts block 2 (offset 2048) short at 3000, block 3 entirely
check_contains "verify: flags the truncated blocks" 1 "FAIL" \
  "$VERIFY" -path "$ft"
check_matches "dump: reports TRUNCATED for the short block" 0 'verdict=TRUNCATED' \
  "$DUMP" -path "$ft"

section "Malformed xattr name: verify/dump must surface it, not drop it silently"
fm="$WORKDIR/malformed-name.dat"
"$SMOKE" -path "$fm" -blocks 2 -blocksize 1024 -kind decimal >/dev/null
# A name that parses as two integers but fails validRange (negative length) --
# has no placeable extent, so it can never become a span; verify/dump must
# still report it rather than silently skip it.
setfattr -n user.verifyio.100--50 -v "0x00" "$fm"
check_contains "verify: reports the malformed record" 1 "malformed iotest xattr record" \
  "$VERIFY" -path "$fm"
check_contains "verify: still reports the sweep's counts alongside it" 1 \
  "2 record(s) verified" "$VERIFY" -path "$fm"
check_contains "dump: reports the malformed record" 0 "MALFORMED" \
  "$DUMP" -path "$fm"

# The scenario above writes 2 valid blocks first, so checkXattrPresence sees
# n == 2 and its `len(malformed) > 0 && n == 0` branch never runs -- the needle
# is satisfied by VerifyFile's MalformedEntriesError instead, and the whole
# check passes against code that never had the fix. A file carrying NOTHING but
# a malformed record is the input that actually reaches it.
fmo="$WORKDIR/malformed-only.dat"
printf 'x' > "$fmo" # non-empty, or checkXattrPresence returns before looking
setfattr -n user.verifyio.100--50 -v "0x00" "$fmo"
check_contains "verify: reports a malformed-only file as corrupt metadata" 1 \
  "malformed iotest xattr record" "$VERIFY" -path "$fmo"
# The discriminating half. Before ForEachEntryStrict, a file whose only records
# were malformed counted zero and fell through to the missing-xattr-support
# diagnosis -- sending the operator to a mount option when the finding was
# corrupt metadata in verifyio's own namespace, on a filesystem supporting
# xattrs perfectly well. Asserting the malformed message alone cannot catch
# that, since it is printed either way.
check_lacks "verify: does not misdiagnose it as missing xattr support" 1 \
  "Filesystem does not support user xattrs" "$VERIFY" -path "$fmo"
check_contains "dump: reports the malformed-only record" 0 "MALFORMED" \
  "$DUMP" -path "$fmo"
# The name is raw listxattr bytes, so a planted newline used to let it compose
# whole lines of output -- including a byte-exact forged span line and a line
# beginning "PASS:" -- in a tool whose exit is always 0 and whose printed text
# IS the verdict. Names must render quoted.
ffg="$WORKDIR/forged-name.dat"
printf 'x' > "$ffg"
setfattr -n "$(printf 'user.verifyio.x\nPASS: forged\noffset=0        length=1024    verdict=OK')" \
  -v "0x00" "$ffg"
check_lacks_matching "dump: a planted newline cannot forge a PASS line" 0 \
  '^PASS:' "$DUMP" -path "$ffg"
check_lacks_matching "dump: a planted newline cannot forge a span line" 0 \
  '^offset=[0-9]+ +length=' "$DUMP" -path "$ffg"
check_lacks_matching "verify: same, on the tool whose exit code is gated on" 1 \
  '^PASS:' "$VERIFY" -path "$ffg"

section "verify: the verdict contract a wrapping script parses"
# One exit code per verdict, and a machine-readable last line with a fixed field
# set. Both are the contract with wrapping scripts, so both are pinned here
# against the real binary rather than only in the unit tests.
fe="$WORKDIR/empty.dat"
: > "$fe"
# Zero coverage is never a pass: nothing was verified, so a caller grepping for
# PASS must not find one, and the exit code must say why.
check_contains "verify: an empty file is NO_DATA, not PASS" 4 "verdict=NO_DATA" \
  "$VERIFY" -path "$fe"
check_lacks "verify: NO_DATA does not print PASS" 4 "PASS" \
  "$VERIFY" -path "$fe"
fs1="$WORKDIR/summary-line.dat"
"$SMOKE" -path "$fs1" -blocks 3 -blocksize 1024 -kind decimal >/dev/null
check_contains "verify: a clean sweep counts records, not spans" 0 \
  "verdict=PASS records=3 gaps=0 contended=0 anomalies=0" "$VERIFY" -path "$fs1"
# A file whose records leave a hole: the gap is read but claims no record, so it
# counts as a gap and never as coverage. Counting it as coverage is what let a
# sweep that verified nothing print PASS at exit 0.
fg="$WORKDIR/gap.dat"
"$SMOKE" -path "$fg" -blocks 2 -blocksize 1024 -kind decimal >/dev/null
truncate -s 4096 "$fg" # extends past the last record: bytes no record claims
check_contains "verify: a trailing hole counts as a gap, not as coverage" 0 \
  "verdict=PASS records=2 gaps=1 contended=0 anomalies=0" "$VERIFY" -path "$fg"
# A missing-xattr-support diagnosis is about the environment, not the data, so
# it gets ERROR's own exit code rather than sharing FAIL's.
fns="$WORKDIR/no-records.dat"
printf 'not written by an iotest tool' > "$fns"
check_contains "verify: a file with no records is ERROR, not FAIL" 5 "verdict=ERROR" \
  "$VERIFY" -path "$fns"

section "iotest-util write: produces a file smoke's own tools can verify/dump"
fu="$WORKDIR/util-write.dat"
check_contains "util write" 0 "kind=prng" \
  "$UTIL" -path "$fu" -blocksize 1024 -blocks 6 write -kind prng
check_contains "verify: util's own output is clean" 0 "PASS" \
  "$VERIFY" -path "$fu"
check_contains "dump: reports all 6 blocks written by util" 0 "6 block(s)" \
  "$DUMP" -path "$fu"
check_contains "dump: reports the kind util was asked for" 0 "kind=prng" \
  "$DUMP" -path "$fu"

section "iotest-util xattr-capacity: probes the per-inode xattr ceiling"
fx="$WORKDIR/xattr-capacity.dat"
# The tool exits 0 whether or not it finds a ceiling (see xattrcap.go), so the
# exit code asserts nothing. Neither does any fixed string that appears in both
# outcomes -- an earlier version of this check grepped for "blocks", which is in
# the `path=... blocksize=... blocks=...` header the tool echoes before it probes
# anything, so it matched no matter what happened.
#
# Which outcome to expect is genuinely not decidable here: the numbers in the
# package doc (~2437 records on tmpfs, ~31 on ext4) are the *listxattr* ceiling,
# while xattr-capacity measures *setxattr* only -- and setxattr on tmpfs accepted
# 44,000+. So 5000 blocks hits a limit on ext4 and sails past it on tmpfs, and
# WORKDIR may be either. Require one of the two real verdicts.
check_matches "util xattr-capacity: reports a definite outcome" 0 \
  "xattrs written before failure|wrote all [0-9]+ xattrs without error" \
  "$UTIL" -path "$fx" -blocksize 4096 -blocks 5000 xattr-capacity

section "Re-run hygiene: a smaller re-run must not inherit the previous run's records"
# Neither writer opens with O_TRUNC (smoke main.go:92-95, util write.go:56-59):
# O_TRUNC resets the data but leaves every xattr record on the inode, so a
# re-run at a smaller size would strand records claiming blocks that no longer
# exist. Both call Writer.Truncate(0) instead, which clears both halves. Every
# other fixture in this script uses a fresh path, so this is the only place a
# tool is re-run against a path it already wrote.
frr="$WORKDIR/rerun-smoke.dat"
"$SMOKE" -path "$frr" -blocks 8 -blocksize 1024 >/dev/null
"$SMOKE" -path "$frr" -blocks 2 -blocksize 1024 >/dev/null
check_contains "verify: a smaller smoke re-run leaves no stale records" 0 \
  "verdict=PASS records=2" "$VERIFY" -path "$frr"
fru="$WORKDIR/rerun-util.dat"
"$UTIL" -path "$fru" -blocksize 1024 -blocks 6 write >/dev/null
"$UTIL" -path "$fru" -blocksize 1024 -blocks 2 write >/dev/null
check_contains "verify: a smaller util write re-run leaves no stale records" 0 \
  "verdict=PASS records=2" "$VERIFY" -path "$fru"

section "Destructive-op guard: every writing tool must refuse a path that is not ours"
# CheckSafeToDestroy (xattrstore/safepath.go) is what stands between a mistyped
# -path and a destroyed real file on a box where these tools run as root. The
# function has unit tests; its three call sites had none, and deleting any one
# of them leaves this script at a clean pass.
#
# The fixture must be a path no verifyio tool has ever succeeded on: the guard
# accepts anything already carrying user.verifyio.* xattrs, and those survive a
# rewrite of the file's contents. Do not reuse this path above, and do not fix
# a failure here by pointing it at an existing fixture.
fguard="$WORKDIR/not-a-verifyio-file.txt"
printf 'not written by an iotest tool\n' > "$fguard"
# All three are 2, and that is the point. These used to be 2/1/1: smoke went
# through climain.Fail(FailUsage) -> 2, while iotest-util went through
# climain.Die -> 1, which was defensible only because util's exit code carries
# no verdict. climain.Die has since been deleted -- it was the one shared helper
# that could exit 1, and therefore the one hole no go/ast guard could see -- so
# every refusal now classifies as FailUsage. Three near-identical checks, one
# number: if one of these drifts off 2, something re-introduced a second way to
# exit from a refused path.
check_contains "smoke: refuses a path that is not a verifyio artifact" 2 \
  "refusing to destroy" "$SMOKE" -path "$fguard" -blocks 2 -blocksize 1024
check_contains "util write: refuses a path that is not a verifyio artifact" 2 \
  "refusing to destroy" "$UTIL" -path "$fguard" -blocksize 1024 -blocks 2 write
check_contains "util xattr-capacity: refuses a path that is not a verifyio artifact" 2 \
  "refusing to destroy" "$UTIL" -path "$fguard" -blocksize 1024 -blocks 8 xattr-capacity
# The property that actually matters: all three refusals left the file alone.
# Covers both destruction shapes -- smoke and write truncate and rewrite,
# xattr-capacity unlinks -- which an exit code and a message cannot tell apart.
check "the refused file is untouched by all three" 0 \
  grep -qx 'not written by an iotest tool' "$fguard"

section "Summary"
printf '%d passed, %d failed\n' "$pass" "$fail"
if [ "$fail" -ne 0 ]; then
  exit 1
fi
