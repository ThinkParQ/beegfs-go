package msg

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

// StartChunkBalanceMsg represents a message for chunk balancing
type StartChunkBalanceMsg struct {
	IdType         RebalanceIDType
	RelativePath   []byte
	TargetIDs      []uint16
	DestinationIDs []uint16
	EntryInfo      *EntryInfo
}

type RebalanceIDType uint8

const (
	RebalanceIDTypeInvalid = iota
	RebalanceIDTypeTarget
	RebalanceIDTypeGroup
	RebalanceIDTypePool
)

func (t RebalanceIDType) String() string {
	switch t {
	case RebalanceIDTypeTarget:
		return "target"
	case RebalanceIDTypeGroup:
		return "group"
	case RebalanceIDTypePool:
		return "pool"
	default:
		return fmt.Sprintf("unknown (%d)", t)
	}
}

// Serialization of StartChunkBalanceMsg
func (m *StartChunkBalanceMsg) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.IdType)
	beeserde.SerializeCStr(s, m.RelativePath, 0)
	beeserde.SerializeSeq(s, m.TargetIDs, true, func(out uint16) {
		beeserde.SerializeInt(s, out)
	})
	beeserde.SerializeSeq(s, m.DestinationIDs, true, func(out uint16) {
		beeserde.SerializeInt(s, out)
	})
	m.EntryInfo.Serialize(s)
}

// MsgId returns the message ID for StartChunkBalanceMsg
func (m *StartChunkBalanceMsg) MsgId() uint16 {
	return 2127
}

func (m *StartChunkBalanceRespMsg) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.Result)
}

// StartChunkBalanceRespMsg represents a response message for starting chunk balance
type StartChunkBalanceRespMsg struct {
	Result beegfs.OpsErr
}

// Serialization of StartChunkBalanceRespMsg
func (m *StartChunkBalanceRespMsg) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.Result)
}

// MsgId returns the message ID for StartChunkBalanceRespMsg
func (m *StartChunkBalanceRespMsg) MsgId() uint16 {
	return 2128
}
