package reconciler

import (
	"fmt"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
	"github.com/thinkparq/protobuf/go/beegfs"
	"go.uber.org/zap"
)

type Config struct {
	ManifestPath       string `mapstructure:"manifest-path"`
	ActiveManifestPath string `mapstructure:"active-manifest-path"`
}

type Configurer interface {
	GetReconcilerConfig() Config
}

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
	agentID string
	log     *zap.Logger
	mu      sync.Mutex
	fs      manifest.Filesystem
	state   state
	config  Config
}

func New(agentID string, log *zap.Logger, config Config) Reconciler {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(defaultReconciler{}).PkgPath())))
	// Setting the initial config and file system manifest will be triggered later by ConfigMgr.
	return &defaultReconciler{
		agentID: agentID,
		log:     log,
		state:   newAgentState(),
		mu:      sync.Mutex{},
	}
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

func (r *defaultReconciler) verify(newFS manifest.Filesystem) error {
	// TODO:
	// * Avoid necessary reconciliations by seeing if the manifest changed.
	// * Validate we can migrate from currentFS to newFS.
	r.log.Info("verifying manifest")
	go r.reconcile(newFS)
	return nil
}

func (r *defaultReconciler) reconcile(fs manifest.Filesystem) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log.Info("starting reconciliation")
	r.log.Debug("reconciling", zap.Any("filesystem", fs))
	ctx := r.state.start()
	ctx.Err()
	// TODO: Reconcile
	r.fs = fs
	manifest.ToDisk(r.fs, r.config.ActiveManifestPath)
	r.state.complete(beegfs.AgentStatus_SUCCESS)
	r.log.Info("completed reconciliation")
}
