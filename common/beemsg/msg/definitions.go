package msg

import "github.com/thinkparq/gobee/beemsg/beeserde"

// Authenticates a TCP connection. Must be sent before sending any other messages. beemsg.NodeStore
// handles this automatically, no extra action needed.
type AuthenticateChannel struct {
	AuthSecret int64
}

func (m *AuthenticateChannel) MsgId() uint16 {
	return 4007
}

func (m *AuthenticateChannel) Serialize(sd *beeserde.SerDes) {
	beeserde.SerializeInt(sd, m.AuthSecret)
}

func (m *AuthenticateChannel) Deserialize(sd *beeserde.SerDes) {
	beeserde.DeserializeInt(sd, &m.AuthSecret)
}

type HeartbeatRequest struct {
}

func (m *HeartbeatRequest) MsgId() uint16 {
	return 1019
}

func (m *HeartbeatRequest) Serialize(sd *beeserde.SerDes) {
}

func (m *HeartbeatRequest) Deserialize(sd *beeserde.SerDes) {
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

func (m *Heartbeat) Deserialize(sd *beeserde.SerDes) {
	beeserde.DeserializeInt(sd, &m.InstanceVersion)
	beeserde.DeserializeInt(sd, &m.NicListVersion)
	beeserde.DeserializeInt(sd, &m.NodeType)
	beeserde.DeserializeCStr(sd, &m.NodeAlias, 0)
	beeserde.DeserializeCStr(sd, &m.AckId, 4)
	beeserde.DeserializeInt(sd, &m.NodeNumId)
	beeserde.DeserializeInt(sd, &m.RootNumId)
	beeserde.DeserializeInt(sd, &m.IsRootMirrored)
	beeserde.DeserializeInt(sd, &m.Port)
	beeserde.DeserializeInt(sd, &m.PortUnused)
	beeserde.DeserializeSeq[Nic](sd, &m.NicList, false, func(out *Nic) {
		out.Deserialize(sd)
	})

}

type Nic struct {
	ipv4    uint32
	alias   []byte
	nicType uint8
}

func (t *Nic) Deserialize(des *beeserde.SerDes) {
	beeserde.DeserializeInt(des, &t.ipv4)

	t.alias = make([]byte, 16)
	beeserde.DeserializeBytes(des, &t.alias)

	beeserde.DeserializeInt(des, &t.nicType)
	beeserde.Skip(des, 3)
}
