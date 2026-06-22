#!/bin/bash
set -u
ts=$(date +%s)
base=/mnt/beegfs/${ts}_fail
sudo mkdir -p "$base"
for i in $(seq 1 5); do
  sudo bash -c "echo failbait > $base/f${i}.dat"
done
# Bogus RST id 99 -> job should hit failed state
sudo beegfs-old --tls-disable rst push --remote-target=99 "$base" 2>&1 || true
echo "fail load $base done"
