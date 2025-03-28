package metadata

import (
	"encoding/binary"
	"fmt"

	pb "github.com/thinkparq/protobuf/go/beewatch"
)

// deserializeEvent takes a buffer and the number of bytes to deserialize from that buffer into an
// event. This allows it to work with a reusable fixed sized buffer to minimize allocations. If
// successful it will return a pointer to a new event that should be reused until sent to all
// subscribers. The the format of the deserialized event is defined as part of the external API at:
// https://github.com/ThinkParQ/protobuf/blob/main/proto/beewatch.proto.
//
// To verify data integrity, the caller should provided the expectedSize of the event that will be
// parsed from the provided buffer so it can be compared with the number of bytes actually parsed.
func deserializeEvent(buf []byte, expectedSize uint32) (*pb.Event, error) {

	var event pb.Event
	// Checking the length of the buffer is pointless since we should be working with a reusable
	// fixed sized buffer that should always be larger than the expected size.

	// Deserialize the header to determine the event version. This is necessary because while the
	// event protocol is determined by the initial handshake, the metadata persistent message queue
	// might contain events with different versions, for example if the meta service was upgraded.
	version := binary.LittleEndian.Uint16(buf[0:2])
	if version == 1 {
		// There was only one v1 packet format that had uint16 major and uint16 minor versions but
		// the only version to ever exist was 1.0. To reduce packet sizes the v2 packets switched to
		// using a single uint16 version. If this is a v1 packet we need to check the minor version.
		minor := binary.LittleEndian.Uint16(buf[2:4])
		if minor != 0 {
			return &event, fmt.Errorf("received unsupported event version (major version: %d, minor version: %d)", version, minor)
		}
		// v1 packets included the size of the event data immediately after the version and
		// expectedSize simply indicates how many bytes were read from the Unix socket.
		size := binary.LittleEndian.Uint32(buf[4:8])
		if expectedSize < size {
			// If the size parsed from the packet is smaller than the expected size we should bail,
			// otherwise stale data in the buffer may be interpreted as fields for this event.
			return &event, fmt.Errorf("only the event header could be deserialized because the provided buffer was smaller than the indicated event size (expected size: %d, actual size: %d)", size, expectedSize)
		}
		event.EventData = parseV1Event(buf)
	} else if version == 2 {
		var bytesParsed uint32
		event.EventFlags = binary.LittleEndian.Uint32(buf[2:6])
		event.EventData, bytesParsed = parseV2Event(buf)
		// The v2 protocol no longer includes the size as part of the event data since this is
		// stored separately in the PMQ for each event. The size is now provided as expectedSize and
		// we simply verify the number of bytes parsed matches expectedSize.
		if bytesParsed != expectedSize {
			return &event, fmt.Errorf("expected packet size was %d but only parsed %d bytes", expectedSize, bytesParsed)
		}
	} else {
		return &event, fmt.Errorf("received unsupported event version %d", version)
	}
	return &event, nil
}

func parseV2Event(buf []byte) (*pb.Event_V2, uint32) {
	entryID, ParentEntryID, path, targetPath, targetParentID, nextOffset := parseCStrings(buf, 18)
	eventData := &pb.Event_V2{
		V2: &pb.V2Event{
			NumLinks:       binary.LittleEndian.Uint64(buf[6:14]),
			Type:           pb.V2Event_Type(binary.LittleEndian.Uint32(buf[14:18])),
			EntryId:        entryID,
			ParentEntryId:  ParentEntryID,
			Path:           path,
			TargetPath:     targetPath,
			TargetParentId: targetParentID,
			MsgUserId:      binary.LittleEndian.Uint32(buf[nextOffset : nextOffset+4]),
			Timestamp:      int64(binary.LittleEndian.Uint64(buf[nextOffset+4 : nextOffset+12])),
			// The end of the uint64 is 8 bytes plus 4 bytes for the previous uint32. Note the
			// casting from uint64 to int64 is intentional, Go will handle the interpretation
			// correctly if the original value was supposed to represent a negative number.
		},
	}
	// Return the last offset so the number of bytes parsed can be compared with the expected bytes.
	return eventData, nextOffset + 12
}

func parseV1Event(buf []byte) *pb.Event_V1 {
	entryID, ParentEntryID, path, targetPath, targetParentID, _ := parseCStrings(buf, 28)
	eventData := &pb.Event_V1{
		V1: &pb.V1Event{
			DroppedSeq:     binary.LittleEndian.Uint64(buf[8:16]),
			MissedSeq:      binary.LittleEndian.Uint64(buf[16:24]),
			Type:           pb.V1Event_Type(binary.LittleEndian.Uint32(buf[24:28])),
			EntryId:        entryID,
			ParentEntryId:  ParentEntryID,
			Path:           path,
			TargetPath:     targetPath,
			TargetParentId: targetParentID,
		},
	}
	return eventData
}

const (
	uint32Size = 4
)

// parseCStrings is used to parse several strings encoded as c-strings from both V1 and V2 event
// buffers. Each string starts with a uint32 value indicating the length of the string to follow.
// Each string is terminated by a NULL character (\x00). The starting offset of these strings inside
// the buffer is different in v1 versus v2 and is set by eventPacketHeaderSize. Additionally V2 also
// needs to known the offset of the next byte to continue parsing the remaining fields which is
// returned as nextOffset.
func parseCStrings(buf []byte, eventPacketHeaderSize int) (entryID, parentEntryID, path, targetPath, targetParentID string, nextOffset uint32) {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		fmt.Println("panic parsing CStrings: ", r)
	// 		fmt.Printf("dumping current state: entryID %s, parentEntryID: %s, path: %s, targetPath: %s, targetParentID: %s, nextOffset: %d\n\nbuffer (as string): %q \n\nbuffer (as hex): \n%s\n\n",
	// 			entryID, parentEntryID, path, targetPath, targetParentID, nextOffset, buf, hex.Dump(buf))
	// 		os.Exit(1)
	// 	}
	// }()
	nextOffset = uint32(eventPacketHeaderSize)
	entryID = parseCString(buf, &nextOffset)
	parentEntryID = parseCString(buf, &nextOffset)
	path = parseCString(buf, &nextOffset)
	// While only some event types like rename have a targetPath/targetParentID, we still need to
	// parse the remaining sizes so we can set nextOffset correctly.
	targetPath = parseCString(buf, &nextOffset)
	targetParentID = parseCString(buf, &nextOffset)
	return
}

func parseCString(buf []byte, offset *uint32) string {
	strLength := binary.LittleEndian.Uint32(buf[*offset : *offset+uint32Size])
	*offset += uint32Size
	str := string(buf[*offset : *offset+strLength])
	// The length doesn't include the null terminator so we need to move the offset to the location
	// where any remaining data in the buffer is expected.
	*offset += strLength + 1
	return str
}
