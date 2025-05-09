package reconciler

import (
	"fmt"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/beegfs-go/agent/pkg/deploy"
	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
	"github.com/thinkparq/protobuf/go/beegfs"
	"go.uber.org/zap"
)

type Config struct {
	ManifestPath       string   `mapstructure:"manifest-path"`
	ActiveManifestPath string   `mapstructure:"active-manifest-path"`
	DeploymentStrategy Strategy `mapstructure:"deployment-strategy"`
}

type Strategy string

const (
	DefaultStrategy Strategy = "default"
)

type Configurer interface {
	GetReconcilerConfig() Config
}

// Reconciler is responsible for comparing the current/active Filesystem with the new desired
// Filesystem and decides what needs to be created, updated, or destroyed.
type Reconciler interface {
	GetAgentID() string
	GetFsUUID() string
	Status() (ReconcileResult, error)
	Cancel(string) (ReconcileResult, error)
	UpdateConfiguration(any) error
}

type ReconcileResult struct {
	Status *beegfs.AgentStatus
}

type defaultReconciler struct {
	agentID   string
	log       *zap.Logger
	mu        sync.Mutex
	currentFS manifest.Filesystem
	state     state
	config    Config
	strategy  deploy.Deployer
}

func New(agentID string, log *zap.Logger, config Config) (Reconciler, error) {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(defaultReconciler{}).PkgPath())))
	var deploymentStrategy deploy.Deployer
	var err error
	switch config.DeploymentStrategy {
	case DefaultStrategy:
		if deploymentStrategy, err = deploy.NewDefaultStrategy(); err != nil {
			return nil, fmt.Errorf("unable to configure deployment strategy: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown deployment strategy: %v", config.DeploymentStrategy)
	}
	// Setting the initial config and file system manifest will be triggered later by ConfigMgr.
	return &defaultReconciler{
		agentID:  agentID,
		log:      log,
		state:    newAgentState(log),
		mu:       sync.Mutex{},
		strategy: deploymentStrategy,
	}, nil
}

func (r *defaultReconciler) GetAgentID() string {
	return r.agentID
}

func (r *defaultReconciler) GetFsUUID() string {
	return "TODO"
}

func (r *defaultReconciler) Status() (ReconcileResult, error) {
	return ReconcileResult{
		Status: r.state.get(),
	}, nil
}

func (r *defaultReconciler) Cancel(reason string) (ReconcileResult, error) {
	r.state.cancel(reason)
	return r.Status()
}

// UpdateConfiguration handles:
//
//   - Local config updates from ConfigMgr where the new manifest is loaded from disk.
//   - Remote config updates from the gRPC server where a new manifest is saved to disk.
//
// In both cases it will verify the new manifest and attempt to reconcile it if possible.
func (r *defaultReconciler) UpdateConfiguration(config any) error {
	if configurer, ok := config.(Configurer); ok {
		r.mu.Lock()
		r.config = configurer.GetReconcilerConfig()
		r.log.Info("loading file system manifest", zap.String("path", r.config.ManifestPath))
		newFS, err := manifest.FromDisk(r.config.ManifestPath)
		r.mu.Unlock()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrLoadingManifest, err)
		}
		return r.verify(newFS)
	} else if newFS, ok := config.(manifest.Filesystem); ok {
		r.mu.Lock()
		r.log.Info("saving file system manifest", zap.String("path", r.config.ActiveManifestPath))
		err := manifest.ToDisk(newFS, r.config.ManifestPath)
		r.mu.Unlock()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadManifest, err)
		}
		return r.verify(newFS)
	}
	return fmt.Errorf("received unexpected reconciler configuration (most likely this indicates a bug and a report should be filed)")
}

// Verify performs any checks that can be done without actually reconciling the manifest. This
// allows a response to be returned quickly while the reconciliation happens in the background.
func (r *defaultReconciler) verify(newFS manifest.Filesystem) error {
	r.log.Info("verifying manifest")
	newFS.InheritGlobalConfig()
	// TODO:
	// * Avoid necessary reconciliations by seeing if the manifest changed.
	// * Validate we can migrate from currentFS to newFS.
	go r.reconcile(newFS)
	return nil
}

// Reconcile attempts to move the local state from the currentFS to the newFS.
func (r *defaultReconciler) reconcile(newFS manifest.Filesystem) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log.Debug("reconciling", zap.Any("filesystem", newFS))
	ctx := r.state.start()

	agent, ok := newFS.Agents[r.agentID]
	if !ok {
		r.state.cancel("no configuration for this agent found in the provided manifest")
		return
	}

	if err := r.strategy.AddInterfaces(ctx, agent.Interfaces); err != nil {
		r.state.fail(err.Error())
		return
	}

	// TODO

	r.currentFS = newFS
	manifest.ToDisk(r.currentFS, r.config.ActiveManifestPath)
	r.state.complete(beegfs.AgentStatus_SUCCESS)
}
