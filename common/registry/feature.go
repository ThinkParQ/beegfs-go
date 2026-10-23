package registry

const (
	FeatureFilterFiles = "filter-files"
	// FeatureRestorePolicyAndCooldown covers per-job configuration options for push/pull
	// (--restore-policy, --remote-cooldown).
	FeatureRestorePolicyAndCooldown = "restore-policy-and-cooldown"
	// FeatureSyncDrain covers draining a sync node before it shuts down. A draining sync node reports
	// DRAINING in its heartbeat.
	FeatureSyncDrain = "sync-drain"
	// FeatureRemoteLockRelease covers remote deciding every release of a file's content lock. Remote
	// attempts a release at the end of every SubmitJob and serves ReleaseUnusedFileLock, and a sync
	// node's job builder relies on both instead of clearing the lock itself. Remote and sync each
	// refuse a peer without it, because a builder talking to a remote without it would leave locks
	// set.
	FeatureRemoteLockRelease = "remote-lock-release"
)
