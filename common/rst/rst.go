// Package RST (Remote Storage Target) implements wrapper types for working with
// RSTs internally and clients for interacting with various RSTs.
//
// Most RST configuration is defined using protocol buffers, however changes to
// this package are needed when adding new RSTs:
//
//   - Add a new typed constant with the internal type of the RST.
//   - Expand the map of SupportedRSTTypes to include the new RST.
//   - Add a new type for the RST that implements the Client interface.
//   - Add the RST type to the New function().
//   - Add support for the new RST type to the desired Job types
//     (via their allocate/deallocate methods).
//
// Note once a new RST type is added, changes to its fields largely should not
// require changes to this package (if everything is setup correctly).
package rst

import (
	"context"
	"fmt"

	"github.com/thinkparq/protobuf/go/flex"
)

// The type of an RST is determined by the oneof field in the
// RemoteStorageTarget protocol buffer defined message. We alo internally
// maintain a separate typed constant for each RST type to simplify determining
// what RST type we're working with, notably for the purposes of generating the
// appropriate segments. This also allows mocking the behavior of any real RST
// type by using a Mock client that returns the type of the desired RST.
type Type string

const (
	S3 Type = "s3"
)

// SupportedRSTTypes is used with SetRSTTypeHook in the config package to allows
// configuring with multiple RST types without writing repetitive code. The map
// contains the all lowercase string identifier of the prefix key of the TOML
// table used to indicate the configuration options for a particular RST type.
// For each RST type a function must be returned that can be used to construct
// the actual structs that will be set to the Type field. The first return value
// is a new struct that satisfies the isRemoteStorageTarget_Type interface. The
// second return value is the address of the struct that is a named field of the
// first return struct and contains the actual message fields for that RST type.
// Note returning the address is important otherwise you will get an initialized
// but empty struct of the correct type.
var SupportedRSTTypes = map[string]func() (any, any){
	"s3": func() (any, any) { t := new(flex.RemoteStorageTarget_S3_); return t, &t.S3 },
	// Azure is not currently supported, but this is how an Azure type could be added:
	// "azure": func() (any, any) { t := new(flex.RemoteStorageTarget_Azure_); return t, &t.Azure },
	// Mock could be included here if it ever made sense to allow configuring them using a file.
}

type Client interface {
	GetType() Type
	CreateUpload(path string) (uploadID string, err error)
	AbortUpload(uploadID string, path string) error
	FinishUpload(uploadID string, path string, parts []*flex.WorkResponse_Part) error
	RecommendedSegments(fileSize int64) (numberOfSegments int64, partsPerSegment int32)
	UploadPart(uploadID string, part int32, path string, offsetStart int64, offsetStop int64) (string, error)
	DownloadPart(path string, offsetStart int64, offsetStop int64) error
	// ExecuteWorkRequestPart accepts a request and which part of the request it should carry out.
	// It blocks until the request is complete, but the caller can cancel the provided context to
	// return early. It determines and executes the requested operation (if supported) then directly
	// updates the part with the results and marks it as completed. If the context is cancelled it
	// does not return an error, but rather updates any fields in the part that make sense to allow
	// the request to be resumed later (if supported), but will not mark the part as completed.
	ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.WorkResponse_Part) error
}

func New(config *flex.RemoteStorageTarget) (Client, error) {
	switch config.Type.(type) {
	case *flex.RemoteStorageTarget_S3_:
		return newS3(config)
	case *flex.RemoteStorageTarget_Mock:
		// Note this handles setting up the MockClient for the purposes of
		// WorkerMgr. However if the RST needs to actually be used (such as
		// calls to Allocate()) then expectations must be setup (see mock.go).
		return &MockClient{}, nil
	case nil:
		return nil, fmt.Errorf("unable to determine RST type from configuration (did you include configuration for a specific RST type? is that type supported yet?): %s", config)
	default:
		// This means we got a valid RST type that was unmarshalled from a TOML
		// file base on SupportedRSTTypes or directly provided to us in a test,
		// but New() doesn't know about it yet.
		return nil, fmt.Errorf("specified RST type is unknown (most likely this is a bug): %T", config.Type)
	}
}
