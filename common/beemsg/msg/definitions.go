package msg

import "github.com/thinkparq/gobee/beemsg/ser"

// Authenticates a TCP connection. Must be sent before sending any other messages. beemsg.NodeStore
// handles this automatically, no extra action needed.
type AuthenticateChannel struct {
	AuthSecret int64
}

func (m *AuthenticateChannel) MsgId() uint16 {
	return 4007
}

func (m *AuthenticateChannel) Serialize(sd *ser.SerDes) {
	ser.SerializeInt(sd, m.AuthSecret)
}

func (m *AuthenticateChannel) Deserialize(sd *ser.SerDes) {
	ser.DeserializeInt(sd, &m.AuthSecret)
}

type HeartbeatRequest struct {
}

func (m *HeartbeatRequest) MsgId() uint16 {
	return 1019
}

func (m *HeartbeatRequest) Serialize(sd *ser.SerDes) {
}

func (m *HeartbeatRequest) Deserialize(sd *ser.SerDes) {
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

func (m *Heartbeat) Deserialize(sd *ser.SerDes) {
	ser.DeserializeInt(sd, &m.InstanceVersion)
	ser.DeserializeInt(sd, &m.NicListVersion)
	ser.DeserializeInt(sd, &m.NodeType)
	ser.DeserializeCStr(sd, &m.NodeAlias, 0)
	ser.DeserializeCStr(sd, &m.AckId, 4)
	ser.DeserializeInt(sd, &m.NodeNumId)
	ser.DeserializeInt(sd, &m.RootNumId)
	ser.DeserializeInt(sd, &m.IsRootMirrored)
	ser.DeserializeInt(sd, &m.Port)
	ser.DeserializeInt(sd, &m.PortUnused)
	ser.DeserializeSeq[Nic](sd, &m.NicList, false, func(out *Nic) {
		out.Deserialize(sd)
	})

}

type Nic struct {
	ipv4    uint32
	alias   []byte
	nicType uint8
}

func (t *Nic) Deserialize(des *ser.SerDes) {
	ser.DeserializeInt(des, &t.ipv4)

	t.alias = make([]byte, 16)
	ser.DeserializeBytes(des, &t.alias)

	ser.DeserializeInt(des, &t.nicType)
	ser.Skip(des, 3)
}

//	type GetNodes struct {
//		NodeType uint32
//	}
//
//	func (t *GetNodes) ID() uint16 {
//		return 1017
//	}
//
//	func (t *GetNodes) Serialize(sd *ser.SerDes) {
//		ser.SerializeInt(sd, t.NodeType)
//	}
//
//	func (t *GetNodes) Deserialize(sd *ser.SerDes) {
//		ser.DeserializeInt(sd, &t.NodeType)
//	}
//
//	type GetNodesResp struct {
//		nodes          []Node
//		rootNodeID     uint32
//		isRootMirrored uint8
//	}
//
//	func (t *GetNodesResp) Deserialize(sd *ser.SerDes) {
//		ser.DeserializeSeq[Node](sd, &t.nodes, false, func(out *Node) {
//			out.Deserialize(sd)
//		})
//		ser.DeserializeInt(sd, &t.rootNodeID)
//		ser.DeserializeInt(sd, &t.isRootMirrored)
//	}
//
//	type Node struct {
//		alias    []byte
//		nics     []Nic
//		nodeID   uint32
//		portUdp  uint16
//		portTcp  uint16
//		nodeType uint8
//	}
//
//	func (t *Node) Deserialize(des *ser.SerDes) {
//		ser.DeserializeCStr(des, &t.alias, 0)
//		ser.DeserializeSeq[Nic](des, &t.nics, false, func(out *Nic) {
//			out.Deserialize(des)
//		})
//		ser.DeserializeInt(des, &t.nodeID)
//		ser.DeserializeInt(des, &t.portUdp)
//		ser.DeserializeInt(des, &t.portTcp)
//		ser.DeserializeInt(des, &t.nodeType)
//	}

//
// func (t *GetNodesResp) ID() uint16 {
// 	return 1018
// }
