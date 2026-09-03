package workmgr

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/beeremote"
	pbr "github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
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
