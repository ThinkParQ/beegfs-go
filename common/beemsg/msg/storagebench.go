package msg

import (
	"github.com/thinkparq/beegfs-go/v8/common/beegfs"
	"github.com/thinkparq/beegfs-go/v8/common/beemsg/beeserde"
)

type StorageBenchControlMsg struct {
	Action    beegfs.StorageBenchAction
	Type      beegfs.StorageBenchType
	BlockSize int64
	FileSize  int64
	Threads   int32
	ODirect   bool
	TargetIDs []uint16
}

func (m *StorageBenchControlMsg) MsgId() uint16 {
	return 1037
}

func (m *StorageBenchControlMsg) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.Action)
	beeserde.SerializeInt(s, m.Type)
	beeserde.SerializeInt(s, m.BlockSize)
	beeserde.SerializeInt(s, m.FileSize)
	beeserde.SerializeInt(s, m.Threads)
	beeserde.SerializeInt(s, m.ODirect)
	beeserde.SerializeSeq(s, m.TargetIDs, true, func(in uint16) {
		beeserde.SerializeInt(s, in)
	})
}

type StorageBenchControlMsgResp struct {
	Status        beegfs.StorageBenchStatus
	Action        beegfs.StorageBenchAction
	Type          beegfs.StorageBenchType
	ErrorCode     beegfs.StorageBenchError
	TargetIDs     []uint16
	TargetResults []int64
}

func (m *StorageBenchControlMsgResp) MsgId() uint16 {
	return 1038
}

func (m *StorageBenchControlMsgResp) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Status)
	beeserde.DeserializeInt(d, &m.Action)
	beeserde.DeserializeInt(d, &m.Type)
	beeserde.DeserializeInt(d, &m.ErrorCode)
	beeserde.DeserializeSeq(d, &m.TargetIDs, true, func(out *uint16) {
		beeserde.DeserializeInt(d, out)
	})
	beeserde.DeserializeSeq(d, &m.TargetResults, true, func(out *int64) {
		beeserde.DeserializeInt(d, out)
	})
}
