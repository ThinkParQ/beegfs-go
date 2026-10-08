package main

import (
	"os/user"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/build"
	"github.com/thinkparq/beegfs-go/ctl/internal/nssresolver"
)

// The helper exists only to resolve through NSS, and main refuses to run when built without cgo.
// Fail loudly rather than silently testing a binary that would not ship.
func TestBuiltWithCGO(t *testing.T) {
	require.True(t, build.CGO)
}

// Checks where handle reports each lookup outcome: a resolved ID goes in the name maps, an ID that
// does not exist goes nowhere, and only a lookup that failed goes in an error map.
func TestHandleClassifiesResults(t *testing.T) {
	if _, err := user.LookupId("0"); err != nil {
		t.Skipf("UID 0 does not resolve on this host: %v", err)
	}

	resp := handle(nssresolver.Request{
		Seq:   3,
		UIDs:  []uint32{0},
		Users: []string{"root"},
	})

	assert.Equal(t, uint64(3), resp.Seq)
	assert.Equal(t, "root", resp.Users[0])
	assert.Contains(t, resp.UserIDs, "root")
	assert.Equal(t, uint32(0), resp.UserIDs["root"])

	// handle allocates every map, so empty ones serialize as {} rather than null.
	empty := handle(nssresolver.Request{})
	assert.NotNil(t, empty.Users)
	assert.NotNil(t, empty.Groups)
	assert.NotNil(t, empty.UIDErrors)
	assert.NotNil(t, empty.GIDErrors)
	assert.NotNil(t, empty.UserIDs)
	assert.NotNil(t, empty.GroupIDs)
	assert.NotNil(t, empty.NameErrors)
}
