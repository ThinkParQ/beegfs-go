package nssresolver

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The CLI and the helper share these types, so they always agree with each other. These tests
// instead pin the JSON itself, so that changing it fails here as a reminder to bump Version.

// Checks that Request marshals to the JSON the helper expects.
func TestRequestWireFormat(t *testing.T) {
	b, err := json.Marshal(Request{Seq: 7, UIDs: []uint32{0, 1000}, Users: []string{"alice"}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"seq":7,"uids":[0,1000],"users":["alice"]}`, string(b))
}

// Checks that Response marshals to the JSON the CLI expects, including numeric map keys and empty
// maps sent as {}.
func TestResponseWireFormat(t *testing.T) {
	b, err := json.Marshal(Response{
		Seq:        7,
		Users:      map[uint32]string{0: "root"},
		Groups:     map[uint32]string{},
		UIDErrors:  map[uint32]string{4242: "connection refused"},
		GIDErrors:  map[uint32]string{},
		UserIDs:    map[string]uint32{"alice": 1000},
		GroupIDs:   map[string]uint32{},
		NameErrors: map[string]string{},
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"seq":7,"users":{"0":"root"},"groups":{},`+
		`"uid_errors":{"4242":"connection refused"},"gid_errors":{},`+
		`"user_ids":{"alice":1000},"group_ids":{},"name_errors":{}}`, string(b))
}
