package agent

import (
	"context"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/thinkparq/protobuf/go/beegfs"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
}

type Reconciler interface {
	Apply(ctx context.Context, host *beegfs.Host) (ReconcileResult, error)
	Destroy(ctx context.Context, host *beegfs.Host) (ReconcileResult, error)
	Status(ctx context.Context) (ReconcileResult, error)
}

type ReconcileResult struct {
	Status *beegfs.AgentStatus
}

type defaultReconciler struct {
	log             *zap.Logger
	mu              sync.RWMutex
	config          Config
	currentState    beegfs.AgentStatus
	historicalState map[time.Time]beegfs.AgentStatus
}

// TODO (current): Wrap the zap.Logger with an intermediate handler that pushes a structured message
// into the currentState.Messages then also logs out the message. By default only info and above
// should be added to the messages.
//
// Then follow the standard that when we enter Apply/Destroy we push the currentState to
// historicalState and start a new currentState that is used to collect the events that happen
// during the current reconcilation loop.

func New(log *zap.Logger, config Config) Reconciler {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(defaultReconciler{}).PkgPath())))
	return &defaultReconciler{
		log:    log,
		config: config,
		currentState: beegfs.AgentStatus{
			State:    beegfs.AgentStatus_IDLE,
			Messages: []string{"[AGENT] Startup"},
			Updated:  timestamppb.Now(),
		},
		historicalState: make(map[time.Time]beegfs.AgentStatus),
	}
}

func (r *defaultReconciler) Apply(ctx context.Context, host *beegfs.Host) (ReconcileResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO
	return ReconcileResult{}, nil
}

func (r *defaultReconciler) Destroy(ctx context.Context, host *beegfs.Host) (ReconcileResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO
	return ReconcileResult{}, nil
}

func (r *defaultReconciler) Status(ctx context.Context) (ReconcileResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return ReconcileResult{
		Status: proto.Clone(&r.currentState).(*beegfs.AgentStatus),
	}, nil
}
