package buddygroup

import (
	"context"

	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/config"
	pm "github.com/thinkparq/protobuf/go/management"
)

// Sends a request to mgmtd to set up metadata mirroring for the root directory
func MirrorRootInode(ctx context.Context) error {
	mgmtd, err := config.ManagementClient()
	if err != nil {
		return err
	}

	_, err = mgmtd.MirrorRootInode(ctx, &pm.MirrorRootInodeRequest{})
	if err != nil {
		return err
	}

	return nil
}
