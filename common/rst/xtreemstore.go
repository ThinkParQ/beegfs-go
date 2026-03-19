package rst

import (
	"context"
	"fmt"
	"time"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// XtreemStoreClient is a dedicated provider type for xtreemstore targets. It currently reuses the
// S3 provider implementation while keeping a separate type so xtreemstore-specific behavior can be
// added without coupling those changes to S3Client.
type XtreemStoreClient struct {
	S3Client *S3Client
}

var _ Provider = &XtreemStoreClient{}

// newXtreemstore initializes an xtreemstore provider by reusing the S3 client implementation.
func newXtreemstore(ctx context.Context, rstConfig *flex.RemoteStorageTarget, mountPoint filesystem.Provider) (Provider, error) {
	xtreemstore := rstConfig.GetXtreemstore()
	if xtreemstore == nil || xtreemstore.GetS3() == nil {
		return nil, fmt.Errorf("xtreemstore configuration must include s3 settings")
	}

	client, err := newS3WithProvider(ctx, rstConfig, xtreemstore.GetS3(), mountPoint, nil)
	if err != nil {
		return nil, err
	}

	s3Client, ok := client.(*S3Client)
	if !ok {
		return nil, fmt.Errorf("unexpected xtreemstore provider type: %T", client)
	}

	return &XtreemStoreClient{S3Client: s3Client}, nil
}

func (x *XtreemStoreClient) GetJobRequest(cfg *flex.JobRequestCfg) *beeremote.JobRequest {
	return x.S3Client.GetJobRequest(cfg)
}

func (x *XtreemStoreClient) GenerateWorkRequests(ctx context.Context, lastJob *beeremote.Job, job *beeremote.Job, availableWorkers int) (requests []*flex.WorkRequest, err error) {
	return x.S3Client.GenerateWorkRequests(ctx, lastJob, job, availableWorkers)
}

func (x *XtreemStoreClient) ExecuteJobBuilderRequest(ctx context.Context, workRequest *flex.WorkRequest, jobSubmissionChan chan<- *beeremote.JobRequest) (reschedule bool, err error) {
	return x.S3Client.ExecuteJobBuilderRequest(ctx, workRequest, jobSubmissionChan)
}

func (x *XtreemStoreClient) ExecuteWorkRequestPart(ctx context.Context, request *flex.WorkRequest, part *flex.Work_Part) error {
	return x.S3Client.ExecuteWorkRequestPart(ctx, request, part)
}

func (x *XtreemStoreClient) CompleteWorkRequests(ctx context.Context, job *beeremote.Job, workResults []*flex.Work, abort bool) error {
	return x.S3Client.CompleteWorkRequests(ctx, job, workResults, abort)
}

func (x *XtreemStoreClient) GetConfig() *flex.RemoteStorageTarget {
	return x.S3Client.GetConfig()
}

func (x *XtreemStoreClient) GetWalk(ctx context.Context, path string, chanSize int, resumeToken string, maxRequests int) (<-chan *filesystem.StreamPathResult, error) {
	return x.S3Client.GetWalk(ctx, path, chanSize, resumeToken, maxRequests)
}

func (x *XtreemStoreClient) SanitizeRemotePath(remotePath string) string {
	return x.S3Client.SanitizeRemotePath(remotePath)
}

func (x *XtreemStoreClient) GetRemotePathInfo(ctx context.Context, cfg *flex.JobRequestCfg) (remoteSize int64, remoteMtime time.Time, isArchived bool, isArchiveRestoreAllowed bool, err error) {
	return x.S3Client.GetRemotePathInfo(ctx, cfg)
}

func (x *XtreemStoreClient) GenerateExternalId(ctx context.Context, cfg *flex.JobRequestCfg) (externalId string, err error) {
	return x.S3Client.GenerateExternalId(ctx, cfg)
}

func (x *XtreemStoreClient) IsWorkRequestReady(ctx context.Context, request *flex.WorkRequest) (ready bool, delay time.Duration, err error) {
	return x.S3Client.IsWorkRequestReady(ctx, request)
}
