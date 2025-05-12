package reconciler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type state struct {
	logger     *zap.Logger
	current    pb.AgentStatus
	historical map[time.Time]*pb.AgentStatus
	mu         sync.Mutex
	ctx        context.Context
	ctxCancel  context.CancelFunc
}

func newAgentState(l *zap.Logger) state {
	return state{
		current: pb.AgentStatus{
			State:    pb.AgentStatus_IDLE,
			Messages: []string{},
			Updated:  timestamppb.Now(),
		},
		historical: make(map[time.Time]*pb.AgentStatus),
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
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", pb.AgentStatus_APPLYING.String()))
	s.historical[time.Now()] = proto.Clone(&s.current).(*pb.AgentStatus)
	s.current = pb.AgentStatus{
		State:    pb.AgentStatus_APPLYING,
		Messages: []string{},
		Updated:  timestamppb.Now(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.ctxCancel = cancel
	s.logUnlocked("began reconciliation")
	return s.ctx
}

func (s *state) get() *pb.AgentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(&s.current).(*pb.AgentStatus)
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

func (s *state) fail(message string) *pb.AgentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", pb.AgentStatus_FAILED.String()), zap.Any("message", message))
	s.current.State = pb.AgentStatus_FAILED
	s.logUnlocked(message)
	s.ctxCancel()
	return proto.Clone(&s.current).(*pb.AgentStatus)
}

func (s *state) cancel(message string) *pb.AgentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", pb.AgentStatus_CANCELLED.String()), zap.Any("message", message))
	s.current.State = pb.AgentStatus_CANCELLED
	s.logUnlocked(message)
	s.ctxCancel()
	return proto.Clone(&s.current).(*pb.AgentStatus)
}

func (s *state) complete(finalState pb.AgentStatus_State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", finalState.String()))
	s.current.State = finalState
	s.logUnlocked("finished reconciliation")
	s.ctxCancel()
}
