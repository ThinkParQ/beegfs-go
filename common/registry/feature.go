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
	// FeatureRSTManagement covers changing remote storage targets and backends while Remote and the
	// sync nodes run. Remote and every sync node advertise it, and each check guards a different
	// link:
	//   - The CLI requires it from Remote before it uses the RPCs that create, update, delete or
	//     adopt targets, or create, update, delete or verify backends. It also requires it before it
	//     relies on what came with them: the deleted targets GetRSTConfig returns, and the reason
	//     UpdateJobs records. An older Remote ignores both requests without an error.
	//   - Remote requires it from every sync node, and refuses an older node when it connects. Remote
	//     pushes the target set after every change. A sync node with the feature applies the set
	//     while it runs, and starts no work request while it holds a set it refused. An older node
	//     refuses every change, and keeps running its queued work with targets Remote no longer
	//     serves.
	// A sync node serves none of the management RPCs. It advertises the feature because the live
	// push is part of management, and both sides shipped in the same release.
	FeatureRSTManagement = "rst-management"
)
