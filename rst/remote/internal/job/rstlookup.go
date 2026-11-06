package job

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/rstdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The job manager looks an RST up for two purposes, and a deleted RST answers them differently:
//   - New work (a job being submitted or scheduled) is refused, with the reason the RST was
//     deleted. See rstForNewWork.
//   - Undoing work already started (the RST abort a cancel runs) uses the deleted RST's retired
//     provider. A download's abort restores the local file through the provider, so without it a
//     cancelled download would leave a half-written file. See rstForUndo.
//
// A job of a deleted RST is cancelled the next time it reaches either kind of lookup. Nothing scans
// for such jobs when the RST is deleted.

// deletedRSTMessage describes a deleted RST for job messages and refusals.
func deletedRSTMessage(id uint32, deleted *rstdb.Deleted) string {
	msg := fmt.Sprintf("remote target %d (%s) was deleted", id, deleted.Config.GetName())
	if deleted.Reason != "" {
		msg += ": " + deleted.Reason
	}
	return msg
}

// rstForNewWork returns the provider to start new work on RST id. For an RST that was deleted it
// returns the delete record and a FailedPrecondition status that names the reason, so the caller
// that submitted the work sees why. For an ID that was never configured it returns a plain error.
func (m *Manager) rstForNewWork(id uint32) (rst.Provider, *rstdb.Deleted, error) {
	provider, deleted, ok := m.workerManager.GetRST(id)
	switch {
	case !ok:
		return nil, nil, fmt.Errorf("rejecting job because the requested RST does not exist: %d", id)
	case deleted != nil:
		return nil, deleted, status.Error(codes.FailedPrecondition, "rejecting job because "+deletedRSTMessage(id, deleted))
	}
	return provider, nil, nil
}

// rstForUndo returns the provider to undo work already started on RST id: the live provider, or
// the retired one of a deleted RST, in which case deleted is its delete record. ok is false for an
// ID that was never configured, and for a deleted RST whose retired provider could not be built.
func (m *Manager) rstForUndo(id uint32) (provider rst.Provider, deleted *rstdb.Deleted, ok bool) {
	provider, deleted, ok = m.workerManager.GetRST(id)
	if !ok || provider == nil {
		return nil, deleted, false
	}
	return provider, deleted, true
}
