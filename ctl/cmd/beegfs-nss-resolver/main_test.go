package main

import (
	"encoding/json"
	"errors"
	"os/user"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/build"
)

// The helper exists only to resolve through NSS, and main refuses to run when built without cgo.
// Fail loudly rather than silently testing a binary that would not ship.
func TestBuiltWithCGO(t *testing.T) {
	require.True(t, build.CGO)
}

// ctl/internal/cmd/quota keeps its own copy of the request and response types, because this
// package is main and cannot be imported. Its quota_test.go checks that copy against the same
// literal used here, so the two definitions have to agree on every field name.
const wireResponse = `{"seq":7,"users":{"0":"root"},"groups":{},` +
	`"uid_errors":{"4242":"connection refused"},"gid_errors":{},` +
	`"user_ids":{"alice":1000},"group_ids":{},"name_errors":{}}`

// Checks that response marshals to the JSON the CLI expects.
func TestResponseWireFormat(t *testing.T) {
	b, err := json.Marshal(response{
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
	assert.JSONEq(t, wireResponse, string(b))
}

// Checks that request unmarshals the JSON the CLI sends, and that the arrays it left out stay nil.
func TestRequestWireFormat(t *testing.T) {
	var req request
	require.NoError(t, json.Unmarshal([]byte(`{"seq":7,"uids":[0,1000],"users":["alice"]}`), &req))

	assert.Equal(t, uint64(7), req.Seq)
	assert.Equal(t, []uint32{0, 1000}, req.UIDs)
	assert.Equal(t, []string{"alice"}, req.Users)
	assert.Nil(t, req.GIDs)
	assert.Nil(t, req.Groups)
}

// Checks where handle reports each lookup outcome: a resolved ID goes in the name maps, an ID that
// does not exist goes nowhere, and only a lookup that failed goes in an error map.
func TestHandleClassifiesResults(t *testing.T) {
	const unknownID = 4294967294

	if _, err := user.LookupId("0"); err != nil {
		t.Skipf("UID 0 does not resolve on this host: %v", err)
	}

	resp := handle(request{
		Seq:   3,
		UIDs:  []uint32{0, unknownID},
		Users: []string{"root"},
	})

	assert.Equal(t, uint64(3), resp.Seq)
	assert.Equal(t, "root", resp.Users[0])
	assert.Equal(t, uint32(0), resp.UserIDs["root"])
	assert.NotContains(t, resp.Users, uint32(unknownID))

	// Only check this when the OS itself reports "not found". A backend that returns an error for
	// the same ID is a valid outcome, and recording it in UIDErrors is then the correct behavior.
	var unknown user.UnknownUserIdError
	if _, err := user.LookupId(strconv.Itoa(unknownID)); errors.As(err, &unknown) {
		assert.NotContains(t, resp.UIDErrors, uint32(unknownID))
	}

	// handle allocates every map, so empty ones serialize as {} rather than null.
	empty := handle(request{})
	assert.NotNil(t, empty.Users)
	assert.NotNil(t, empty.Groups)
	assert.NotNil(t, empty.UIDErrors)
	assert.NotNil(t, empty.GIDErrors)
	assert.NotNil(t, empty.UserIDs)
	assert.NotNil(t, empty.GroupIDs)
	assert.NotNil(t, empty.NameErrors)
}
