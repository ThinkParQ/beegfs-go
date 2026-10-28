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
	// attempts a release at the end of every SubmitJob. A sync node's job builder relies on that
	// release instead of clearing the lock itself. Remote and sync each refuse a peer without the
	// feature, because a builder talking to a remote without it would leave locks set.
	FeatureRemoteLockRelease = "remote-lock-release"
	// FeatureReserveBeforeLock covers a sync node's job builder reserving a job before it takes a
	// file's content lock. Remote answers a replayed reservation with the job in whatever state it
	// reached, and the builder resumes from it after a reschedule. Remote undoes a refused claim's
	// changes to the file with the RST abort, and the builder never undoes them itself. A bulk
	// request whose path has no file is held with a locked, empty placeholder until its claim.
	// Remote and sync each refuse a peer without the feature:
	//   - An older remote refuses a replayed reservation that was already claimed, and the builder
	//     would count a path it submitted as refused.
	//   - An older remote does not undo a refused claim, so the claim's changes would stay.
	FeatureReserveBeforeLock = "reserve-before-lock"
)
