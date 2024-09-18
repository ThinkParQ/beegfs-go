package msg

import (
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

// Requests server node stats
type GetHighResStats struct {
	LastStatTime int64
}

func (m *GetHighResStats) MsgId() uint16 {
	return 2051
}

func (m *GetHighResStats) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.LastStatTime)
}

// Server node stats response
type GetHighResStatsResp struct {
	Stats []HighResolutionStats
}

func (m *GetHighResStatsResp) MsgId() uint16 {
	return 2052
}

func (m *GetHighResStatsResp) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeSeq[HighResolutionStats](d, &m.Stats, false, func(inner *HighResolutionStats) {
		inner.Deserialize(d)
	})
}

type HighResolutionStats struct {
	StatsTimeMS    uint64
	BusyWorkers    uint32
	QueuedRequests uint32
	DiskWriteBytes uint64
	DiskReadBytes  uint64
	NetSendBytes   uint64
	NetRecvBytes   uint64
	WorkRequests   uint32
}

func (m *HighResolutionStats) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.StatsTimeMS)
	beeserde.DeserializeInt(d, &m.BusyWorkers)
	beeserde.DeserializeInt(d, &m.QueuedRequests)
	beeserde.DeserializeInt(d, &m.DiskWriteBytes)
	beeserde.DeserializeInt(d, &m.DiskReadBytes)
	beeserde.DeserializeInt(d, &m.NetSendBytes)
	beeserde.DeserializeInt(d, &m.NetRecvBytes)
	beeserde.DeserializeInt(d, &m.WorkRequests)
}

// Requests client stats
type GetClientStats struct {
	CookieIP uint64
	PerUser  bool
}

func (m *GetClientStats) MsgId() uint16 {
	return 1031
}

func (m *GetClientStats) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.CookieIP)

	if m.PerUser {
		s.MsgFeatureFlags = 1
	}
}

// Client stats response
type GetClientStatsResp struct {
	Stats []uint64
}

func (m *GetClientStatsResp) MsgId() uint16 {
	return 1032
}

func (m *GetClientStatsResp) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeSeq[uint64](d, &m.Stats, true, func(inner *uint64) {
		beeserde.DeserializeInt(d, inner)
	})
}
