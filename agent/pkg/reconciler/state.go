package reconciler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thinkparq/protobuf/go/beegfs"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type state struct {
	logger     *zap.Logger
	current    beegfs.AgentStatus
	historical map[time.Time]*beegfs.AgentStatus
	mu         sync.Mutex
	ctx        context.Context
	ctxCancel  context.CancelFunc
}

type op string

const (
	unknown op = "UNKNOWN"
	agent      = "AGENT"
	mount      = "MOUNT"
)

func newAgentState(l *zap.Logger) state {
	return state{
		current: beegfs.AgentStatus{
			State:    beegfs.AgentStatus_IDLE,
			Messages: []string{},
			Updated:  timestamppb.Now(),
		},
		historical: make(map[time.Time]*beegfs.AgentStatus),
		mu:         sync.Mutex{},
		logger:     l,
	}
}

// start() marks the beginning of a reconciliation. It returns a context that will be cancelled if
// the reconciliation is cancelled early.
func (s *state) start() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", beegfs.AgentStatus_APPLYING.String()))
	s.historical[time.Now()] = proto.Clone(&s.current).(*beegfs.AgentStatus)
	s.current = beegfs.AgentStatus{
		State:    beegfs.AgentStatus_APPLYING,
		Messages: []string{},
		Updated:  timestamppb.Now(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.ctxCancel = cancel
	s.logUnlocked(agent, "began reconciliation")
	return s.ctx
}

func (s *state) get() *beegfs.AgentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(&s.current).(*beegfs.AgentStatus)
}

func (s *state) logUnlocked(cat op, message string) {
	s.current.Updated = timestamppb.Now()
	s.current.Messages = append(s.current.Messages, fmt.Sprintf("%s [%s]: %s", s.current.Updated.String(), cat, message))
}

func (s *state) log(cat op, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current.Updated = timestamppb.Now()
	s.current.Messages = append(s.current.Messages, fmt.Sprintf("%s [%s]: %s", s.current.Updated.String(), cat, message))
}

func (s *state) fail(message string) *beegfs.AgentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", beegfs.AgentStatus_FAILED.String()))
	s.current.State = beegfs.AgentStatus_FAILED
	s.logUnlocked(agent, message)
	s.ctxCancel()
	return proto.Clone(&s.current).(*beegfs.AgentStatus)
}

func (s *state) cancel(message string) *beegfs.AgentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", beegfs.AgentStatus_CANCELLED.String()))
	s.current.State = beegfs.AgentStatus_CANCELLED
	s.logUnlocked(agent, message)
	s.ctxCancel()
	return proto.Clone(&s.current).(*beegfs.AgentStatus)
}

func (s *state) complete(finalState beegfs.AgentStatus_State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("state update", zap.String("oldState", s.current.State.String()), zap.String("newState", finalState.String()))
	s.current.State = finalState
	s.logUnlocked(agent, "finished reconciliation")
	s.ctxCancel()
}
