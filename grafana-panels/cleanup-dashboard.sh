#!/bin/bash
# Rollback: delete dashboard + all 5 alert rules. Folder kept (in case other things use it).
set -u
G="http://localhost:3000"
AUTH="admin:admin"
for uid in beegfs-job-failed beegfs-job-backlog beegfs-sync-stuck beegfs-remote-down beegfs-sync-down; do
  echo -n "alert $uid: "
  curl -sS -u "$AUTH" -X DELETE -H 'X-Disable-Provenance: true' \
    "$G/api/v1/provisioning/alert-rules/$uid" -w "HTTP %{http_code}\n"
done
echo -n "dashboard beegfs-remote-sync-exp: "
curl -sS -u "$AUTH" -X DELETE "$G/api/dashboards/uid/beegfs-remote-sync-exp" -w "HTTP %{http_code}\n"
echo "(Folder 'beegfs-demo' kept; delete manually via API/UI if desired.)"
