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
