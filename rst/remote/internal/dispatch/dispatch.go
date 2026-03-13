package dispatch

import (
	"context"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/job"
	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/beewatch"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

type Config struct {
}

type Manager struct {
	log       *zap.Logger
	jobMgr    *job.Manager
	events    <-chan *beewatch.Event
	acks      chan<- subscriber.Ack
	wg        sync.WaitGroup
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func New(log *zap.Logger, jobMgr *job.Manager, events <-chan *beewatch.Event, acks chan<- subscriber.Ack) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	log = log.With(zap.String("component", path.Base(reflect.TypeFor[Manager]().PkgPath())))
	return &Manager{
		log:       log,
		jobMgr:    jobMgr,
		events:    events,
		acks:      acks,
		ctx:       ctx,
		ctxCancel: cancel,
	}
}

func (d *Manager) Start() {
	d.wg.Go(func() {
		for {
			select {
			case <-d.ctx.Done():
				return
			case event := <-d.events:
				d.acks <- d.dispatch(event)
			}
		}
	})
}

func (d *Manager) Stop() {
	d.ctxCancel()
	d.wg.Wait()
	d.log.Info("stopped dispatcher")
}

func (d *Manager) dispatch(event *beewatch.Event) subscriber.Ack {

	ack := subscriber.Ack{
		MetaId: event.MetaId,
		SeqId:  event.SeqId,
	}

	e, ok := event.EventData.(*beewatch.Event_V2)
	if !ok {
		d.log.Warn("event version is unsupported (ignoring)", zap.Any("ack", ack))
		return ack
	} else if e.V2.Type != beewatch.V2Event_OPEN_BLOCKED {
		d.log.Info("filtered out event type (ignoring)", zap.Any("ack", ack), zap.Any("type", e.V2.GetType()))
		return ack
	}

	path := e.V2.GetPath()

	d.log.Info("detected a blocked open event, checking if the stub file is eligible for automatic restore", zap.String("path", path))
	entry, err := entry.GetEntry(d.ctx, nil, entry.GetEntriesCfg{}, e.V2.Path)
	if err != nil {
		d.log.Warn("skipping automatic restore as the stub file's data state could not be determined", zap.String("path", path), zap.Error(err))
		return ack
	}

	if entry.Entry.FileState.GetDataState() != beegfs.DataStateAutoRestore {
		d.log.Info("stub file's data state does not allow an automatic restore", zap.String("path", path), zap.Any("dataState", entry.Entry.FileState.GetDataState()))
		return ack
	}

	// If a restore is currently in progress this will fail for "stub file is malformed". If a
	// restore was already requested but not yet started, then the job submission below is rejected.
	id, url, err := d.jobMgr.GetStubContents(path)
	if err != nil {
		d.log.Warn("skipping automatic restore as the stub file's contents could not be retrieved", zap.String("path", path), zap.Error(err))
		return ack
	}

	result, err := d.jobMgr.SubmitJobRequest(&beeremote.JobRequest{
		Path:                path,
		Priority:            5,
		RemoteStorageTarget: id,
		Type: &beeremote.JobRequest_Sync{
			Sync: &flex.SyncJob{
				Operation:  flex.SyncJob_DOWNLOAD,
				RemotePath: url,
				Overwrite:  true,
			},
		},
	})

	if err != nil {
		d.log.Warn("unable to create sync job to restore the specified stub file's contents", zap.String("path", path), zap.Error(err))
		return ack
	}

	d.log.Info("stub file restore triggered", zap.Any("result", result))

	return ack
}
