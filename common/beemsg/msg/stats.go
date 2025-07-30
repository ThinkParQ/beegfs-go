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
	beeserde.DeserializeSeq(d, &m.Stats, false, func(inner *HighResolutionStats) {
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

// Only to be used by GetClientStatsV2
type Uint128 struct {
	High uint64
	Low  uint64
}

// Requests client stats v2
type GetClientStatsV2 struct {
	CookieID Uint128
	PerUser  bool
}

func (m *GetClientStatsV2) MsgId() uint16 {
	return 1033
}

func (m *GetClientStatsV2) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.CookieID.Low)
	beeserde.SerializeInt(s, m.CookieID.High)

	if m.PerUser {
		s.MsgFeatureFlags = 1
	}
}

// Client stats response v2
type GetClientStatsV2Resp struct {
	Stats []Uint128
}

func (m *GetClientStatsV2Resp) MsgId() uint16 {
	return 1034
}

func (m *GetClientStatsV2Resp) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeSeq(d, &m.Stats, true, func(inner *Uint128) {
		beeserde.DeserializeInt(d, &inner.Low)
		beeserde.DeserializeInt(d, &inner.High)
	})
}

// The below Request*Data messages are only used to determine which message version for requesting
// client stats shall be used (by using a header flag). We don't actually process them, so the
// responses are dummies that only take care of the header flag.

type RequestMetaData struct {
	LastStatsTime int64
}

func (m *RequestMetaData) MsgId() uint16 {
	return 6003
}

func (m *RequestMetaData) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.LastStatsTime)
}

type RequestMetaDataRespDummy struct {
	UseClientStatsV2 bool
}

func (m *RequestMetaDataRespDummy) MsgId() uint16 {
	return 6005
}

func (m *RequestMetaDataRespDummy) Deserialize(d *beeserde.Deserializer) {
	m.UseClientStatsV2 = (d.MsgFeatureFlags & 1) == 1
	d.Buf.Reset()
}

type RequestStorageData struct {
	LastStatsTime int64
}

func (m *RequestStorageData) MsgId() uint16 {
	return 6004
}

func (m *RequestStorageData) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.LastStatsTime)
}

type RequestStorageDataRespDummy struct {
	UseClientStatsV2 bool
}

func (m *RequestStorageDataRespDummy) MsgId() uint16 {
	return 6006
}

func (m *RequestStorageDataRespDummy) Deserialize(d *beeserde.Deserializer) {
	m.UseClientStatsV2 = (d.MsgFeatureFlags & 1) == 1
	d.Buf.Reset()
}
