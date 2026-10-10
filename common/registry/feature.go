package registry

const (
	FeatureFilterFiles = "filter-files"
	// FeatureRestorePolicyAndCooldown covers per-job configuration options for push/pull
	// (--restore-policy, --remote-cooldown).
	FeatureRestorePolicyAndCooldown = "restore-policy-and-cooldown"
	// FeatureSyncDrain covers draining a sync node before it shuts down. A draining sync node reports
	// DRAINING in its heartbeat.
	FeatureSyncDrain = "sync-drain"
)
