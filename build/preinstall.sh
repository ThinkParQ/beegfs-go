#!/bin/sh
set -e

# Check if group 'beegfs' exists, if not, create it.
if ! getent group beegfs >/dev/null; then
  groupadd --system beegfs
fi
