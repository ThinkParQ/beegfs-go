#!/bin/bash
set -u
ts=$(date +%s)
base=/mnt/beegfs/${ts}_happy
sudo mkdir -p "$base/r1" "$base/r2"
for rst in 1 2; do
  d="$base/r${rst}"
  sudo bash -c "for i in \$(seq 1 50); do dd if=/dev/urandom of=$d/f\$i.dat bs=4096 count=1 status=none; done"
  sudo beegfs-old --tls-disable entry set --remote-targets=${rst} "$d" >/dev/null
  sudo beegfs-old --tls-disable rst push --remote-target=${rst} "$d" >/dev/null 2>&1 &
done
wait
# Pull a few back
for rst in 1 2; do
  d="$base/r${rst}"
  sudo beegfs-old --tls-disable rst pull "$d" >/dev/null 2>&1 || true
done
echo "happy load $base done"
