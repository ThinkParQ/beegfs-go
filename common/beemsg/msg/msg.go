package msg

import "github.com/thinkparq/gobee/beemsg/ser"

// A BeeGFS message
type Msg interface {
	// The unique message MsgId for this message
	// Must match the definition in other locations, e.g. the C++ or Rust codebase
	MsgId() uint16
}

// A BeeGFS message that is also serializable
type SerializableMsg interface {
	Msg
	ser.Serializable
}

// A BeeGFS message that is also deserializable
type DeserializableMsg interface {
	Msg
	ser.Deserializable
}
