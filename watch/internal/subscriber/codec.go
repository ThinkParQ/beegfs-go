package subscriber

import (
	"fmt"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

// rawBytesCodec is used as the codec for the subscriber gRPC connection so that pre-marshaled
// []byte values are written to the wire as-is, avoiding a redundant proto.Marshal on the send
// path. It is scoped to the subscriber connection via grpc.ForceCodecV2 and does not affect other
// gRPC connections in the process.
type rawBytesCodec struct{}

func (rawBytesCodec) Marshal(v any) (mem.BufferSlice, error) {
	if b, ok := v.([]byte); ok {
		return mem.BufferSlice{mem.SliceBuffer(b)}, nil
	}
	if m, ok := v.(proto.Message); ok {
		b, err := proto.Marshal(m)
		if err != nil {
			return nil, err
		}
		return mem.BufferSlice{mem.SliceBuffer(b)}, nil
	}
	return nil, fmt.Errorf("rawBytesCodec: cannot marshal %T", v)
}

func (rawBytesCodec) Unmarshal(data mem.BufferSlice, v any) error {
	if m, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data.Materialize(), m)
	}
	return fmt.Errorf("rawBytesCodec: cannot unmarshal into %T", v)
}

// Name must return "proto" to match the server's content-type (application/grpc+proto).
func (rawBytesCodec) Name() string {
	return "proto"
}
