package workermgr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// TestVerifyBackend checks that one verify asks Remote and every node, and turns each answer into
// its outcome. Remote verifies against a local HTTP server that answers like an S3 store.
func TestVerifyBackend(t *testing.T) {
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>`)
	}))
	defer store.Close()
	backend := &flex.RemoteStorageTarget{Name: "site", Type: &flex.RemoteStorageTarget_S3_{S3: &flex.RemoteStorageTarget_S3{
		EndpointUrl: store.URL, Region: "us-east-1", AccessKey: "k", SecretKey: "s",
	}}}

	online := func(id string, verify ...any) worker.Config {
		return worker.Config{ID: id, Name: id, Type: worker.Mock, MaxReconnectBackOff: 1,
			MockConfig: worker.MockConfig{Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{nil}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
				{MethodName: "VerifyBackend", Args: []any{mock.Anything}, ReturnArgs: verify},
			}},
		}
	}
	workerConfigs := []worker.Config{
		online("a", "a note", nil),
		online("b", "", errors.New("the store rejected the credentials")),
		online("c", "", worker.ErrVerifyUnsupported),
		{ID: "d", Name: "d", Type: worker.Mock, MaxReconnectBackOff: 1,
			MockConfig: worker.MockConfig{Expectations: []worker.MockExpectation{
				{MethodName: "connect", ReturnArgs: []any{errors.New("unreachable")}},
				{MethodName: "disconnect", ReturnArgs: []any{nil}},
			}},
		},
	}
	mgr, _, cleanup := newWorkTestManager(t, workerConfigs)
	defer cleanup()
	pool := mgr.nodePools[worker.Mock]
	require.Eventually(t, func() bool {
		for _, id := range []string{"a", "b", "c"} {
			if pool.nodeMap[id].GetState() != worker.ONLINE {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)

	results := mgr.VerifyBackend(context.Background(), backend, 10*time.Second)
	type row struct {
		Host    string
		Outcome beeremote.VerifyRSTBackendResponse_Outcome
		Message string
	}
	got := []row{}
	for _, r := range results {
		got = append(got, row{r.GetHost(), r.GetOutcome(), r.GetMessage()})
	}
	assert.Equal(t, []row{
		{RemoteHost, beeremote.VerifyRSTBackendResponse_OK, ""},
		{"a", beeremote.VerifyRSTBackendResponse_OK, "a note"},
		{"b", beeremote.VerifyRSTBackendResponse_FAILED, "the store rejected the credentials"},
		{"c", beeremote.VerifyRSTBackendResponse_UNSUPPORTED, "the node is too old to verify a backend (hint: upgrade it)"},
		{"d", beeremote.VerifyRSTBackendResponse_OFFLINE, "the node is not connected"},
	}, got, "Remote comes first, then the nodes sorted by ID")
}
