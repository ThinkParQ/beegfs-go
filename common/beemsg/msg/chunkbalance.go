package msg

import (
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

// StartChunkBalanceMsg represents a message for chunk balancing
type StartChunkBalanceMsg struct {
	TargetID      uint16
	DestinationID uint16
	EntryInfo     *EntryInfo
	RelativePaths  *[]string
}

// Serialization of StartChunkBalanceMsg
func (m *StartChunkBalanceMsg) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.TargetID)
	beeserde.SerializeInt(s, m.DestinationID)
	m.EntryInfo.Serialize(s)
	if m.RelativePaths != nil {
		byteSlices := make([][]byte, len(*m.RelativePaths))
		for i, str := range *m.RelativePaths {
			byteSlices[i] = []byte(str)
		}
		beeserde.SerializeStringSeq(s, byteSlices)
	}
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
	Result int32
}

// Serialization of StartChunkBalanceRespMsg
func (m *StartChunkBalanceRespMsg) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.Result)
}

// MsgId returns the message ID for StartChunkBalanceRespMsg
func (m *StartChunkBalanceRespMsg) MsgId() uint16 {
	return 2128
}