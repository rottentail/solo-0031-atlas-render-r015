package service

import (
	"atlas-render/internal/model"
	"atlas-render/internal/raster"
	"atlas-render/internal/scene"
	"fmt"
)

func (s *Service) Submit(snapshotID string, scale int, key string) (model.Job, error) {
	var out model.Job
	if !scene.ValidID(key) {
		return out, model.Invalid("idempotency key required (1..80 identifier characters)")
	}
	fingerprint := model.CanonicalDigest([]any{snapshotID, scale})
	e := s.Store.Update(func(st *model.State) error {
		if r, ok := st.Receipts[key]; ok {
			if r.Fingerprint != fingerprint {
				return model.Conflict("idempotency key used for different request")
			}
			out = st.Jobs[r.JobID]
			return nil
		}
		snap, ok := st.Snapshots[snapshotID]
		if !ok {
			return model.Missing("snapshot not found")
		}
		if snap.State != "active" {
			return model.Conflict("snapshot is revoked")
		}
		if _, e := raster.Compile(snap, scale); e != nil {
			return e
		}
		if pendingJobs(*st, "") >= 64 {
			return model.Busy("render queue is full")
		}
		j, _, e := st.CommitJob(model.JobFact{Transition: model.SubmitJob, Draft: &model.Job{
			ID:         model.ID("job", st.Sequence+1),
			SnapshotID: snapshotID,
			Scale:      scale,
			Payload:    snap,
		}})
		if e != nil {
			return e
		}
		st.Receipts[key] = model.Receipt{Key: key, Fingerprint: fingerprint, JobID: j.ID}
		out = j
		return nil
	})
	return out, e
}
func (s *Service) Cancel(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, _, e := st.CommitJob(model.JobFact{Transition: model.CancelJob, JobID: id})
		out = j
		return e
	})
	return out, e
}
func (s *Service) Retry(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, _, e := st.CommitJob(model.JobFact{Transition: model.RetryJob, JobID: id})
		if e != nil {
			return e
		}
		// Capacity is charged against other pending jobs; the retried job
		// already reads as queued inside this transaction, so exclude it.
		if pendingJobs(*st, id) >= 64 {
			return model.Busy("render queue is full")
		}
		out = j
		return nil
	})
	return out, e
}
func (s *Service) Purge(id string) error {
	return s.Store.Update(func(st *model.State) error {
		a, ok := st.Artifacts[id]
		if !ok {
			return model.Missing("artifact not found")
		}
		if a.State == "purged" {
			return nil
		}
		a.State = "purged"
		a.PNG = nil
		st.Artifacts[id] = a
		st.Emit("artifact.purged", id)
		return nil
	})
}
func (s *Service) Artifact(id string) (model.Artifact, error) {
	a, ok := s.Store.Read().Artifacts[id]
	if !ok {
		return a, model.Missing("artifact not found")
	}
	if a.State == "purged" {
		return a, model.Gone(fmt.Sprintf("artifact %s was purged", id))
	}
	return a, nil
}

// pendingJobs counts the jobs holding queue capacity, excluding exclude.
func pendingJobs(st model.State, exclude string) int {
	n := 0
	for id, j := range st.Jobs {
		if id != exclude && (j.State == model.JobQueued || j.State == model.JobRunning) {
			n++
		}
	}
	return n
}
