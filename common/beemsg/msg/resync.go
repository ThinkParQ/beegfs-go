package msg

import (
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

// Requests storage target resync stats
type GetStorageResyncStats struct {
	TargetID uint16
}

func (m *GetStorageResyncStats) MsgId() uint16 {
	return 2093
}

func (m *GetStorageResyncStats) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.TargetID)
}

type GetStorageResyncStatsResp struct {
	State     BuddyResyncJobState
	StartTime int64
	EndTime   int64
	// Counts the total number of file chunks (files) discovered during the directory traversal.
	DiscoveredFiles uint64
	// Counts the total number of directories discovered during traversal.
	DiscoveredDirs uint64
	// Tracks the number of file chunks identified as sync candidates based on modification time.
	MatchedFiles uint64
	// Tracks the number of directories identified as sync candidates based on modification time.
	MatchedDirs uint64
	SyncedFiles uint64
	SyncedDirs  uint64
	ErrorFiles  uint64
	ErrorDirs   uint64
}

func (m *GetStorageResyncStatsResp) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.State)
	beeserde.DeserializeInt(d, &m.StartTime)
	beeserde.DeserializeInt(d, &m.EndTime)
	beeserde.DeserializeInt(d, &m.DiscoveredFiles)
	beeserde.DeserializeInt(d, &m.DiscoveredDirs)
	beeserde.DeserializeInt(d, &m.MatchedFiles)
	beeserde.DeserializeInt(d, &m.MatchedDirs)
	beeserde.DeserializeInt(d, &m.SyncedFiles)
	beeserde.DeserializeInt(d, &m.SyncedDirs)
	beeserde.DeserializeInt(d, &m.ErrorFiles)
	beeserde.DeserializeInt(d, &m.ErrorDirs)
}

func (m *GetStorageResyncStatsResp) MsgId() uint16 {
	return 2094
}

// request resync stats from the meta targets
type GetMetaResyncStats struct {
	TargetID uint16
}

func (m *GetMetaResyncStats) MsgId() uint16 {
	return 2117
}

func (m *GetMetaResyncStats) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.TargetID)
}

type GetMetaResyncStatsResp struct {
	State     BuddyResyncJobState
	StartTime int64
	EndTime   int64
	// number of directories that have been discovered for sync processing within the dentry structure
	DiscoveredDirs uint64
	// number of errors when crawling directories by BuddyResyncerGatherSlave
	GatherErrors uint64
	// number of directory that has been resynced.
	// If a directory is deleted before the sync, it is still counted to maintain consistency in tracking
	// all intended sync operations.
	SyncedDirs uint64
	// sum of all files in metadata server's buddymir directory and
	// DiscoveredDirs (needed to resync xattrs of dirs by treating them as files)
	SyncedFiles uint64
	// count of errors encountered during directory synchronization, such as issues with opening,
	// locking, or accessing directories..
	ErrorDirs uint64
	// count of errors encountered during file synchronization, including errors from missing files,
	// failed stat operations, etc
	ErrorFiles uint64
	// tracks the number of client sessions to sync
	SessionsToSync uint64
	// tracks the number of client sessions synched
	SyncedSessions uint64
	// boolean flag in beemsg to track if there was any error during synchronization
	SessionSyncErrors uint8
	//  modification objects synced
	ModObjectsSynced uint64
	// modification sync errors
	ModSyncErrors uint64
}

func (m *GetMetaResyncStatsResp) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.State)
	beeserde.DeserializeInt(d, &m.StartTime)
	beeserde.DeserializeInt(d, &m.EndTime)
	beeserde.DeserializeInt(d, &m.DiscoveredDirs)
	beeserde.DeserializeInt(d, &m.GatherErrors)
	beeserde.DeserializeInt(d, &m.SyncedDirs)
	beeserde.DeserializeInt(d, &m.SyncedFiles)
	beeserde.DeserializeInt(d, &m.ErrorDirs)
	beeserde.DeserializeInt(d, &m.ErrorFiles)
	beeserde.DeserializeInt(d, &m.SessionsToSync)
	beeserde.DeserializeInt(d, &m.SyncedSessions)
	beeserde.DeserializeInt(d, &m.SessionSyncErrors)
	beeserde.DeserializeInt(d, &m.ModObjectsSynced)
	beeserde.DeserializeInt(d, &m.ModSyncErrors)

}

func (m *GetMetaResyncStatsResp) MsgId() uint16 {
	return 2118
}

type BuddyResyncJobState int32

const (
	NotStarted BuddyResyncJobState = iota
	Running
	Success
	Interrupted
	Failure
	Errors
)

// Output user friendly string representation
func (n BuddyResyncJobState) String() string {
	switch n {
	case NotStarted:
		return "Not-started"
	case Running:
		return "Running"
	case Success:
		return "Success"
	case Interrupted:
		return "Interrupted"
	case Failure:
		return "Failure"
	case Errors:
		return "Errors"
	default:
		return "<unspecified>"
	}

}
