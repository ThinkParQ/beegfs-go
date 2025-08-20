package msg

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
	pb "github.com/thinkparq/protobuf/go/beewatch"
)

// StartChunkBalanceMsg represents a message for chunk balancing
type StartChunkBalanceMsg struct {
	IdType         RebalanceIDType
	RelativePath   []byte
	TargetIDs      []uint16
	DestinationIDs []uint16
	EntryInfo      *EntryInfo
	// FileEvent must always be set for this message type, and only the Path should be specified.
	// The FileEvent.Type is always INODE_LOCKED and the serialization logic ignores other values
	// specified by the caller. The meta expects this as the initial event type, then it will use
	// this same context to emit a STRIPE_PATTERN_CHANGED if the rebalancing is successful.
	FileEvent *FileEvent
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
	if m.FileEvent != nil {
		// Equivalent of STARTCHUNKBALANCEMSG_FLAG_HAS_EVENT in C++. Unlike the client only includes
		// fileEvent context when configured, the meta always expects us to include event details
		// and the only places that currently use this message (ctl) we intentionally don't even
		// provide the option to disable event logging.
		s.MsgFeatureFlags |= 1
		m.FileEvent.Type = pb.V2Event_INODE_LOCKED
		m.FileEvent.Serialize(s)
	} else {
		s.Fail(fmt.Errorf("unable to serialize StartChunkBalanceMsg without FileEvent context"))
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
