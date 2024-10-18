package msg

import "github.com/thinkparq/beegfs-go/common/beemsg/beeserde"

// Authenticates a TCP connection. Must be sent before sending any other messages. beemsg.NodeStore
// handles this automatically, no extra action needed.
type AuthenticateChannel struct {
	AuthSecret uint64
}

func (m *AuthenticateChannel) MsgId() uint16 {
	return 4007
}

func (m *AuthenticateChannel) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, m.AuthSecret)
}

func (m *AuthenticateChannel) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.AuthSecret)
}

type HeartbeatRequest struct {
}

func (m *HeartbeatRequest) MsgId() uint16 {
	return 1019
}

func (m *HeartbeatRequest) Serialize(s *beeserde.Serializer) {
}

func (m *HeartbeatRequest) Deserialize(d *beeserde.Deserializer) {
}

type Heartbeat struct {
	InstanceVersion uint64
	NicListVersion  uint64
	NodeType        int32
	NodeAlias       []byte
	AckId           []byte
	NodeNumId       uint32
	RootNumId       uint32
	IsRootMirrored  uint8
	Port            uint16
	PortUnused      uint16
	NicList         []Nic
}

func (m *Heartbeat) MsgId() uint16 {
	return 1020
}

func (m *Heartbeat) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &m.InstanceVersion)
	beeserde.DeserializeInt(d, &m.NicListVersion)
	beeserde.DeserializeInt(d, &m.NodeType)
	beeserde.DeserializeCStr(d, &m.NodeAlias, 0)
	beeserde.DeserializeCStr(d, &m.AckId, 4)
	beeserde.DeserializeInt(d, &m.NodeNumId)
	beeserde.DeserializeInt(d, &m.RootNumId)
	beeserde.DeserializeInt(d, &m.IsRootMirrored)
	beeserde.DeserializeInt(d, &m.Port)
	beeserde.DeserializeInt(d, &m.PortUnused)
	beeserde.DeserializeSeq[Nic](d, &m.NicList, false, func(out *Nic) {
		out.Deserialize(d)
	})

}

type Nic struct {
	ipv4    uint32
	alias   []byte
	nicType uint8
}

func (t *Nic) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &t.ipv4)

	t.alias = make([]byte, 16)
	beeserde.DeserializeBytes(d, &t.alias)

	beeserde.DeserializeInt(d, &t.nicType)
	beeserde.Skip(d, 3)
}
