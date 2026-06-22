#!/bin/bash
# Continuous push/pull loop. Run in another terminal during demo.
# Usage:   ./loadgen-loop.sh [files-per-iter] [sleep-seconds]
# Default: 20 files, 30s between iterations.
set -u
FILES=${1:-20}
SLEEP=${2:-30}
echo "loadgen-loop: $FILES files/iter, ${SLEEP}s between, Ctrl-C to stop"
trap 'echo "stopping"; exit 0' INT
while :; do
  ts=$(date +%s)
  for rst in 1 2; do
    d=/mnt/beegfs/loop_${ts}_r${rst}
    sudo mkdir -p "$d"
    sudo bash -c "for i in \$(seq 1 $FILES); do dd if=/dev/urandom of=$d/f\$i.dat bs=4096 count=1 status=none; done"
    sudo beegfs-old --tls-disable entry set --remote-targets=${rst} "$d" >/dev/null
    sudo beegfs-old --tls-disable rst push --remote-target=${rst} "$d" >/dev/null 2>&1 &
  done
  wait
  # Pull half back to drive scheduler + downloads
  for rst in 1 2; do
    d=/mnt/beegfs/loop_${ts}_r${rst}
    sudo beegfs-old --tls-disable rst pull "$d" >/dev/null 2>&1 || true
  done
  echo "$(date +%H:%M:%S) iter $ts done (rst1+rst2 push+pull, $FILES files each)"
  sleep "$SLEEP"
done
