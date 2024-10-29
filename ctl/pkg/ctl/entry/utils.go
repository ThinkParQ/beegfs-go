package entry

import (
	"context"
	"os"

	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
)

// Asynchronously walks a directory from startingPath, immediately returning a channel where the
// paths will be sent, or an error if anything goes wrong setting up.
func walkDir(ctx context.Context, startingPath string, errChan chan<- error) (<-chan string, error) {

	beegfsClient, err := config.BeeGFSClient(startingPath)
	if err != nil {
		return nil, err
	}

	startInMount, err := beegfsClient.GetRelativePathWithinMount(startingPath)
	if err != nil {
		return nil, err
	}

	pathChan := make(chan string, 1024)
	go func() {
		walkDirFunc := func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				pathChan <- path
			}
			return nil
		}

		err := beegfsClient.WalkDir(startInMount, walkDirFunc)
		if err != nil {
			errChan <- err
			close(pathChan)
		}
		close(pathChan)
	}()

	return pathChan, nil
}
