#!/bin/sh
set -e

# Note the order is important otherwise the setgid bit will become unset when the ownership changes.
chown root:beegfs /opt/beegfs/sbin/beegfs
chmod 2755 /opt/beegfs/sbin/beegfs
