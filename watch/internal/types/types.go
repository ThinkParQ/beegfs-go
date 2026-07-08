/*
Package types provides custom data structures meant for internal use in BeeWatch.
*/
package types

import "math"

const (
	// NoSeqId is the sentinel meaning "no sequence ID established yet." Zero is not usable because
	// SeqId=0 is a valid first event from the metadata PMQ.
	NoSeqId uint64 = math.MaxUint64
	// SeekToEndSeqID is the CompletedSeq value a subscriber sends to skip all historical events
	// and start receiving from the next new event. It intentionally shares its value with NoSeqId
	// but has a distinct name to make the two roles clear at each call site.
	SeekToEndSeqID uint64 = math.MaxUint64
	// SendFromStartSeqID is the CompletedSeq value a subscriber sends to request delivery of all
	// events from the beginning of the buffer. Do not store it in lastSeqID: it is not an
	// acknowledgment of any event, and storing 0 would be indistinguishable from "subscriber
	// acknowledged SeqId=0."
	SendFromStartSeqID uint64 = 0
)
