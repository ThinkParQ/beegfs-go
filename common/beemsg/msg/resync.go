package msg

import (
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

// Requests target resyc stats
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
	State           BuddyResyncJobState
	StartTime       int64
	EndTime         int64
	DiscoveredFiles uint64
	DiscoveredDirs  uint64
	MatchedFiles    uint64
	MatchedDirs     uint64
	SyncedFiles     uint64
	SyncedDirs      uint64
	ErrorFiles      uint64
	ErrorDirs       uint64
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
	State             BuddyResyncJobState
	StartTime         int64
	EndTime           int64
	DiscoveredDirs    uint64
	GatherErrors      uint64
	SyncedDirs        uint64
	SyncedFiles       uint64
	ErrorDirs         uint64
	ErrorFiles        uint64
	SessionsToSync    uint64
	SyncedSessions    uint64
	SessionSyncErrors uint8
	ModObjectsSynced  uint64
	ModSyncErrors     uint64
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
		return "Not_Started"
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
