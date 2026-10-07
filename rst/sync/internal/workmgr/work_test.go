package workmgr

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/beeremote"
	pbr "github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSendBuilderJobRequestCounters pins how each submission moves the builder job's counters. A
// path a bulk operation takes is submitted twice: a reserve when the operation takes it and a claim
// once the operation knows what work it needs. It must still be counted once, by the claim's
// outcome, while JobsReserved tracks the reservations whose claim has not been answered yet.
func TestSendBuilderJobRequestCounters(t *testing.T) {
	reserve := func() *pbr.JobRequest {
		request := pbr.JobRequest_builder{Path: "/f", Reserve: true}.Build()
		request.SetReserveJobId("48d3cf27-3e99-4f59-9c49-aec4bdc331be")
		return request
	}
	claim := func() *pbr.JobRequest {
		request := pbr.JobRequest_builder{Path: "/f"}.Build()
		request.SetReserveJobId("48d3cf27-3e99-4f59-9c49-aec4bdc331be")
		return request
	}
	plain := func() *pbr.JobRequest {
		return pbr.JobRequest_builder{Path: "/f"}.Build()
	}

	type submission struct {
		request *pbr.JobRequest
		err     error
	}
	cases := []struct {
		name        string
		submissions []submission
		want        *flex.BuilderJob
	}{
		{
			name:        "an accepted reserve is held, not submitted",
			submissions: []submission{{reserve(), nil}},
			want:        flex.BuilderJob_builder{JobsReserved: 1}.Build(),
		},
		{
			name:        "an accepted claim releases its reservation and counts as submitted",
			submissions: []submission{{reserve(), nil}, {claim(), nil}},
			want:        flex.BuilderJob_builder{Submitted: 1}.Build(),
		},
		{
			name:        "a refused claim releases its reservation and counts its refusal",
			submissions: []submission{{reserve(), nil}, {claim(), rst.ErrReservationMissing}},
			want:        flex.BuilderJob_builder{JobsNotReserved: 1}.Build(),
		},
		{
			name:        "a claim that fails for another reason still releases its reservation",
			submissions: []submission{{reserve(), nil}, {claim(), rst.ErrJobFailedPrecondition}},
			want:        flex.BuilderJob_builder{Errors: 1}.Build(),
		},
		{
			// The reserve never held anything, so nothing is waiting on a claim.
			name:        "a refused reserve counts its refusal and holds nothing",
			submissions: []submission{{reserve(), rst.ErrJobNotAllowed}},
			want:        flex.BuilderJob_builder{JobsNotAllowed: 1}.Build(),
		},
		{
			// A crash loses the counters of a round that had not committed, including its reserves,
			// while the claims for them still arrive. The count must not go negative.
			name:        "a claim with no counted reservation leaves the count at zero",
			submissions: []submission{{claim(), nil}},
			want:        flex.BuilderJob_builder{Submitted: 1}.Build(),
		},
		{
			name:        "a request outside any bulk operation does not touch the count",
			submissions: []submission{{reserve(), nil}, {plain(), nil}},
			want:        flex.BuilderJob_builder{JobsReserved: 1, Submitted: 1}.Build(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := beeremote.New(beeremote.Config{})
			require.NoError(t, err)
			// The address "mock:0" is what makes the client use the mock provider.
			require.NoError(t, client.UpdateConfig(flex.BeeRemoteNode_builder{Address: "mock:0"}.Build(), "0"))
			mockRemote, ok := client.Provider.(*beeremote.MockProvider)
			require.True(t, ok)
			w := &worker{beeRemoteClient: client}

			builder := &flex.BuilderJob{}
			mu := &sync.Mutex{}
			for _, s := range tc.submissions {
				call := mockRemote.On("submitJob", mock.Anything).Return(s.err).Once()
				gotErr := w.sendBuilderJobRequest(context.Background(), mu, builder, s.request)
				if s.err == nil {
					require.NoError(t, gotErr)
				} else {
					require.ErrorIs(t, gotErr, s.err)
				}
				call.Unset()
			}

			assert.Equal(t, tc.want.GetSubmitted(), builder.GetSubmitted(), "submitted")
			assert.Equal(t, tc.want.GetErrors(), builder.GetErrors(), "errors")
			assert.Equal(t, tc.want.GetJobsNotAllowed(), builder.GetJobsNotAllowed(), "not allowed")
			assert.Equal(t, tc.want.GetJobsNotReserved(), builder.GetJobsNotReserved(), "not reserved")
			assert.Equal(t, tc.want.GetJobsReserved(), builder.GetJobsReserved(), "reserved")
		})
	}
}

// TestSendBuilderJobRequestRetries pins which submit failures sendBuilderJobRequest retries. It
// retries only while remote has not decided the request: remote cannot be reached, or the
// connection was closed under a call the caller did not cancel. A refusal remote decided is counted
// once, because remote gives the same answer on every retry. A call the caller cancelled is
// returned without being counted, because nothing is wrong with the request. The retry cases each
// wait out the first one second backoff, so the cases run in parallel.
func TestSendBuilderJobRequestRetries(t *testing.T) {
	unavailable := fmt.Errorf("%w: %w", beeremote.ErrUnavailable, status.Error(codes.Unavailable, "connection refused"))
	canceled := fmt.Errorf("%w: %w", context.Canceled, status.Error(codes.Canceled, "grpc: the client connection is closing"))
	refused := status.Error(codes.Unknown, "unable to generate job from job request")

	cases := []struct {
		name          string
		callerCancels bool
		// results are what remote answers to each submit, in order.
		results []error
		wantErr error
		want    *flex.BuilderJob
	}{
		{
			name:    "a refusal remote decided is counted once and not retried",
			results: []error{refused},
			wantErr: refused,
			want:    flex.BuilderJob_builder{Errors: 1}.Build(),
		},
		{
			name:    "remote that cannot be reached is retried until it accepts",
			results: []error{unavailable, nil},
			want:    flex.BuilderJob_builder{Submitted: 1}.Build(),
		},
		{
			name:    "a connection closed under a live call is retried until remote accepts",
			results: []error{canceled, nil},
			want:    flex.BuilderJob_builder{Submitted: 1}.Build(),
		},
		{
			name:          "a call the caller cancelled is returned and not counted",
			callerCancels: true,
			results:       []error{canceled},
			wantErr:       context.Canceled,
			want:          &flex.BuilderJob{},
		},
		{
			name:          "remote that cannot be reached is not retried once the caller cancels",
			callerCancels: true,
			results:       []error{unavailable},
			wantErr:       beeremote.ErrUnavailable,
			want:          &flex.BuilderJob{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, err := beeremote.New(beeremote.Config{})
			require.NoError(t, err)
			// The address "mock:0" is what makes the client use the mock provider.
			require.NoError(t, client.UpdateConfig(flex.BeeRemoteNode_builder{Address: "mock:0"}.Build(), "0"))
			mockRemote, ok := client.Provider.(*beeremote.MockProvider)
			require.True(t, ok)
			for _, result := range tc.results {
				mockRemote.On("submitJob", mock.Anything).Return(result).Once()
			}
			w := &worker{beeRemoteClient: client, log: zaptest.NewLogger(t)}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.callerCancels {
				cancel()
			}

			builder := &flex.BuilderJob{}
			gotErr := w.sendBuilderJobRequest(ctx, &sync.Mutex{}, builder, pbr.JobRequest_builder{Path: "/f"}.Build())
			if tc.wantErr == nil {
				require.NoError(t, gotErr)
			} else {
				require.ErrorIs(t, gotErr, tc.wantErr)
			}

			mockRemote.AssertNumberOfCalls(t, "submitJob", len(tc.results))
			assert.Equal(t, tc.want.GetSubmitted(), builder.GetSubmitted(), "submitted")
			assert.Equal(t, tc.want.GetErrors(), builder.GetErrors(), "errors")
		})
	}
}

// TestGetBuilderResultsReserved pins what the builder job reports while reservations wait on a
// bulk operation, and when some are left at the end.
func TestGetBuilderResultsReserved(t *testing.T) {
	cfg := &flex.JobRequestCfg{Download: true, RemotePath: "prefix"}

	t.Run("reservations alone are not reported as no matches", func(t *testing.T) {
		// Every path the walk found was taken by a bulk operation, so none has been claimed yet.
		message, hasErrors := getBuilderResults(flex.BuilderJob_builder{Cfg: cfg, JobsReserved: 4446}.Build())
		assert.Equal(t, "4446 reserved and not yet claimed by a bulk operation", message)
		assert.NotContains(t, message, "no matches found")
		// hasErrors is only read once the builder job ends. A reservation left then never got a job.
		assert.True(t, hasErrors)
	})

	t.Run("no line is added once every reservation is claimed", func(t *testing.T) {
		message, hasErrors := getBuilderResults(flex.BuilderJob_builder{Cfg: cfg, Submitted: 4446}.Build())
		assert.Equal(t, "4446 job request(s) submitted", message)
		assert.False(t, hasErrors)
	})
}
