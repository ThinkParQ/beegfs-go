package msg

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
	pb "github.com/thinkparq/protobuf/go/beewatch"
)

type FileEvent struct {
	Type pb.V2Event_Type
	Path []byte
	// targetValid is not defined here and determined depending if target is nil or set.
	Target []byte
}

func (m *FileEvent) Serialize(s *beeserde.Serializer) {
	// pb.V2Event_Type is an int32 (as are all protobuf enums). On the wire, BeeMsg expects a
	// uint32. Any non-negative int32 fits safely in a uint32. Sanity-check for negatives which
	// should never happen since all event types are greater than zero.
	if m.Type < 0 {
		s.Fail(fmt.Errorf("event type must be non-negative to cast to uint32: %d (this is probably a bug)", m.Type))
	}
	beeserde.SerializeInt(s, m.Type)
	beeserde.SerializeCStr(s, m.Path, 0)

	if m.Target != nil {
		// targetValid = true
		beeserde.SerializeInt(s, true)
		beeserde.SerializeCStr(s, m.Target, 0)
	} else {
		// targetValid = false
		beeserde.SerializeInt(s, false)
	}
}
