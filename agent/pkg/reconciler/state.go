package reconciler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/agent"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type state struct {
	logger     *zap.Logger
	current    pb.Status
	historical map[time.Time]*pb.Status
	mu         sync.Mutex
	ctx        context.Context
	ctxCancel  context.CancelFunc
}

func newAgentState(l *zap.Logger) state {
	return state{
		current: pb.Status{
			State:    pb.Status_IDLE,
			Messages: []string{},
			Updated:  timestamppb.Now(),
		},
		historical: make(map[time.Time]*pb.Status),
		mu:         sync.Mutex{},
		logger:     l,
	}
}

func getFsNodeID(fsUUID string, nt beegfs.NodeType, id beegfs.NumId) string {
	return fmt.Sprintf("%s:%s:%d", fsUUID, nt, id)
}

// start() marks the beginning of a reconciliation. It returns a context that will be cancelled if
// the reconciliation is cancelled early.
func (s *state) start() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", pb.Status_APPLYING.String()))
	s.historical[time.Now()] = proto.Clone(&s.current).(*pb.Status)
	s.current = pb.Status{
		State:    pb.Status_APPLYING,
		Messages: []string{},
		Updated:  timestamppb.Now(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.ctxCancel = cancel
	s.logUnlocked("began reconciliation")
	return s.ctx
}

func (s *state) get() *pb.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(&s.current).(*pb.Status)
}

func (s *state) logUnlocked(message string) {
	s.current.Updated = timestamppb.Now()
	s.current.Messages = append(s.current.Messages, fmt.Sprintf("%s: %s", s.current.Updated.String(), message))
}

func (s *state) log(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current.Updated = timestamppb.Now()
	s.current.Messages = append(s.current.Messages, fmt.Sprintf("%s: %s", s.current.Updated.String(), message))
}

func (s *state) fail(message string) *pb.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", pb.Status_FAILED.String()), zap.Any("message", message))
	s.current.State = pb.Status_FAILED
	s.logUnlocked(message)
	s.ctxCancel()
	return proto.Clone(&s.current).(*pb.Status)
}

func (s *state) cancel(message string) *pb.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", pb.Status_CANCELLED.String()), zap.Any("message", message))
	s.current.State = pb.Status_CANCELLED
	s.logUnlocked(message)
	s.ctxCancel()
	return proto.Clone(&s.current).(*pb.Status)
}

func (s *state) complete(finalState pb.Status_State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", finalState.String()))
	s.current.State = finalState
	s.logUnlocked("finished reconciliation")
	s.ctxCancel()
}
