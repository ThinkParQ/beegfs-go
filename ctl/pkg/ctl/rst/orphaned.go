package rst

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CleanupOrphanedCfg struct {
	PathPrefix string
	Recurse    bool
}

type CleanupOrphanedResult struct {
	Path    string
	Deleted bool
	Skipped bool
	Message string
	Err     error
}

// CleanupOrphaned scans the Remote DB for path entries and deletes those that no longer exist in BeeGFS.
func CleanupOrphaned(ctx context.Context, cfg CleanupOrphanedCfg) (<-chan *CleanupOrphanedResult, func() error, error) {
	if cfg.PathPrefix == "" {
		return nil, nil, fmt.Errorf("missing path prefix")
	}

	mountPoint, err := config.BeeGFSClient(cfg.PathPrefix)
	if err != nil {
		return nil, nil, err
	}

	beeRemote, err := config.BeeRemoteClient()
	if err != nil {
		return nil, nil, err
	}

	// Walk the Remote DB once using a streaming GetJobs call.
	dbChan := make(chan *GetJobsResponse, 1024)
	if err := GetJobs(ctx, GetJobsConfig{Path: cfg.PathPrefix, Recurse: cfg.Recurse}, dbChan); err != nil {
		return nil, nil, err
	}

	numWorkers := max(viper.GetInt(config.NumWorkersKey)-1, 1)
	paths := make(chan string, numWorkers*4)
	results := make(chan *CleanupOrphanedResult, numWorkers*4)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(paths)
		for {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			case resp, ok := <-dbChan:
				if !ok {
					return nil
				}
				if resp.Err != nil {
					return resp.Err
				}
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case paths <- resp.Path:
				}
			}
		}
	})

	for range numWorkers {
		g.Go(func() error {
			for {
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case path, ok := <-paths:
					if !ok {
						return nil
					}
					result := cleanupOrphanedPath(gCtx, mountPoint, beeRemote, path)
					select {
					case <-gCtx.Done():
						return gCtx.Err()
					case results <- result:
					}
				}
			}
		})
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- g.Wait()
		close(results)
	}()

	wait := func() error {
		return <-errChan
	}

	return results, wait, nil
}

func cleanupOrphanedPath(ctx context.Context, mountPoint filesystem.Provider, beeRemote beeremote.BeeRemoteClient, dbPath string) *CleanupOrphanedResult {
	result := &CleanupOrphanedResult{Path: dbPath}

	_, err := mountPoint.Lstat(dbPath)
	if err == nil {
		result.Skipped = true
		result.Message = "path exists"
		return result
	}
	if !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, syscall.ENOTDIR) {
		result.Err = err
		return result
	}

	resp, err := beeRemote.UpdateJobs(ctx, beeremote.UpdateJobsRequest_builder{
		Path:        dbPath,
		NewState:    beeremote.UpdateJobsRequest_DELETED,
		ForceUpdate: true,
	}.Build())
	if err != nil {
		if isNotFoundErr(err) || errors.Is(err, rst.ErrEntryNotFound) {
			result.Skipped = true
			result.Message = "entry already removed"
			return result
		}
		result.Err = err
		return result
	}
	result.Message = resp.GetMessage()
	if resp.GetOk() {
		result.Deleted = true
	} else {
		result.Skipped = true
	}
	return result
}

func isNotFoundErr(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.NotFound
}
