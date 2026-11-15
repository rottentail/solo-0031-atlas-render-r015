package engine

import (
	"atlas-render/internal/codec"
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"atlas-render/internal/raster"
	"context"
	"errors"
	"log"
	"sort"
	"sync"
	"time"
)

type Worker struct {
	store  *persist.Store
	mu     sync.Mutex
	active map[string]context.CancelFunc
}

func New(st *persist.Store) *Worker {
	return &Worker{store: st, active: map[string]context.CancelFunc{}}
}
func (w *Worker) Recover() error {
	return w.store.Update(func(s *model.State) error {
		for id := range s.Jobs {
			if _, _, e := s.CommitJob(model.JobFact{Transition: model.RecoverJob, JobID: id}); e != nil {
				return e
			}
		}
		return nil
	})
}
func (w *Worker) Cancel(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if cancel := w.active[id]; cancel != nil {
		cancel()
	}
}
func (w *Worker) Run(ctx context.Context) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if e := w.step(ctx); e != nil {
				log.Printf("render worker: %v", e)
			}
		}
	}
}
func (w *Worker) claim() (model.Job, bool, error) {
	var job model.Job
	found := false
	state := w.store.Read()
	ids := []string{}
	for id, j := range state.Jobs {
		if j.State == model.JobQueued {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return job, false, nil
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := state.Jobs[ids[i]], state.Jobs[ids[j]]
		if a.CreatedAt.Equal(b.CreatedAt) {
			return a.ID < b.ID
		}
		return a.CreatedAt.Before(b.CreatedAt)
	})
	e := w.store.Update(func(s *model.State) error {
		j, committed, e := s.CommitJob(model.JobFact{Transition: model.ClaimJob, JobID: ids[0]})
		if e != nil {
			return e
		}
		if committed {
			job = j
			found = true
		}
		return nil
	})
	return job, found, e
}
func (w *Worker) step(parent context.Context) error {
	j, ok, e := w.claim()
	if e != nil || !ok {
		return e
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	w.mu.Lock()
	w.active[j.ID] = cancel
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.active, j.ID); w.mu.Unlock() }()
	// Cancellation can commit between claim and tracking; consult the committed state.
	current := w.store.Read().Jobs[j.ID]
	if current.State != model.JobRunning {
		cancel()
	}
	plan, e := raster.Compile(j.Payload, j.Scale)
	var data []byte
	if e == nil {
		var imgErr error
		img, imgErr := plan.Render(ctx)
		e = imgErr
		if e == nil {
			data, e = codec.Encode(img)
		}
	}
	if parent.Err() != nil {
		return nil
	}
	return w.store.Update(func(s *model.State) error {
		// The completion fact is fenced to the claimed attempt: a cancel or
		// retry committed meanwhile wins and this fact is dropped.
		fact := model.JobFact{JobID: j.ID, Attempt: j.Attempt}
		switch {
		case e == nil:
			fact.Transition = model.SucceedJob
			fact.Artifact = &model.Artifact{State: "live", Digest: model.Digest(data), Width: plan.Width, Height: plan.Height, PNG: data}
		case errors.Is(e, context.Canceled):
			fact.Transition = model.AbortJob
		default:
			fact.Transition = model.FailJob
			fact.Error = e.Error()
		}
		_, _, cerr := s.CommitJob(fact)
		return cerr
	})
}
