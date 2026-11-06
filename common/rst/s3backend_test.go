package rst

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/protobuf/go/flex"
)

// s3ErrorBody is the body a store sends with an error status.
const s3ErrorBody = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`

// s3ListBucketsBody is the body a store sends for a ListBuckets request that succeeds.
const s3ListBucketsBody = `<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>o</ID></Owner><Buckets></Buckets></ListAllMyBucketsResult>`

// s3BackendFor returns a backend whose endpoint is url.
func s3BackendFor(url string) *flex.RemoteStorageTarget {
	s3 := s3Connection()
	s3.SetEndpointUrl(url)
	return &flex.RemoteStorageTarget{Name: "site", Type: &flex.RemoteStorageTarget_S3_{S3: s3}}
}

func TestVerifyS3Backend(t *testing.T) {
	for name, tc := range map[string]struct {
		status  int
		body    string
		note    string
		wantErr string
	}{
		"a list passes":                         {status: http.StatusOK, body: s3ListBucketsBody},
		"keys that may not list pass with note": {status: http.StatusForbidden, body: fmt.Sprintf(s3ErrorBody, "AccessDenied", "denied"), note: "may not list buckets"},
		"an unknown key fails":                  {status: http.StatusForbidden, body: fmt.Sprintf(s3ErrorBody, "InvalidAccessKeyId", "no such key"), wantErr: "the store rejected the credentials: InvalidAccessKeyId"},
		"a wrong secret fails":                  {status: http.StatusForbidden, body: fmt.Sprintf(s3ErrorBody, "SignatureDoesNotMatch", "bad signature"), wantErr: "the store rejected the credentials: SignatureDoesNotMatch"},
		"another error fails":                   {status: http.StatusServiceUnavailable, body: fmt.Sprintf(s3ErrorBody, "SlowDown", "busy"), wantErr: "the store refused the request"},
	} {
		t.Run(name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				assert.Equal(t, "/", r.URL.Path, "ListBuckets names no bucket")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()

			backend := s3BackendFor(server.URL)
			note, err := VerifyBackend(context.Background(), backend)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.Contains(t, note, tc.note)
			}
			assert.Equal(t, 1, requests, "verify sends one request and never retries")
		})
	}
}

func TestVerifyS3BackendUnreachable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	url := "http://" + listener.Addr().String()
	listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = VerifyBackend(ctx, s3BackendFor(url))
	require.ErrorContains(t, err, "is unreachable")
}

func TestVerifyBackendChecksTheBackendFirst(t *testing.T) {
	backend := s3BackendFor("https://s3.example.com")
	backend.GetS3().SetBucket("b")
	_, err := VerifyBackend(context.Background(), backend)
	require.ErrorContains(t, err, "cannot set bucket", "an invalid backend never reaches the store")
}
