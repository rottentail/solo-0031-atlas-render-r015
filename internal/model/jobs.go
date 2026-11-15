package model

import (
	"fmt"
	"time"
)

// Job lifecycle states committed to the persistent state.
const (
	JobQueued    = "queued"
	JobRunning   = "running"
	JobSucceeded = "succeeded"
	JobFailed    = "failed"
	JobCancelled = "cancelled"
)

// JobTransition names a committed fact in the job lifecycle. Every change to
// a job's committed state — submit, claim, cancel, retry, startup recovery
// and executor completion — is expressed as one of these transitions and
// committed through State.CommitJob, so the whole protocol is reviewed in
// this one file instead of being assembled at each call site.
type JobTransition string

const (
	// SubmitJob creates a job: it commits as queued with attempt 1.
	SubmitJob JobTransition = "submit"
	// ClaimJob hands a queued job to the executor.
	ClaimJob JobTransition = "claim"
	// CancelJob is an operator cancel of a queued or running job.
	CancelJob JobTransition = "cancel"
	// RetryJob re-queues a failed or cancelled job as a new attempt with
	// the same fixed input.
	RetryJob JobTransition = "retry"
	// RecoverJob returns a running job to queued at startup without
	// starting a new attempt.
	RecoverJob JobTransition = "recover"
	// SucceedJob commits the executor's result for the claimed attempt.
	SucceedJob JobTransition = "succeed"
	// FailJob commits the executor's failure for the claimed attempt.
	FailJob JobTransition = "fail"
	// AbortJob commits the executor's observation that the claimed attempt
	// was cancelled underneath it.
	AbortJob JobTransition = "abort"
)

// jobRule is one row of the job lifecycle protocol: which committed states a
// transition may start from, what it commits, and how a fact that arrives
// against any other state is answered.
type jobRule struct {
	event string   // event kind emitted when the transition commits
	to    string   // state the transition commits
	from  []string // committed states the transition may start from; nil marks creation
	fence bool     // commit only when the fact carries the currently committed attempt
	bump  bool     // commit starts a new attempt and clears the previous error
	noop  bool     // re-applying the transition to its target state is a confirmed no-op
	deny  string   // conflict reported for other states; empty drops the fact silently
}

// jobProtocol is the single reviewable statement of the job lifecycle:
//
//	submit ─▶ queued ──claim──▶ running ──succeed──▶ succeeded
//	           ▲ ▲               │ └────────fail────▶ failed ─┐
//	           │ └──recover──────┴───────abort───▶ cancelled ◀┘
//	           └──────────────retry──────────────────────────┘
//	(operator cancel commits from queued|running and lands on cancelled)
//
// Two invariants fall out of the table and hold for every writer: cancel and
// success are singular committed facts — once either is committed the other
// can never replace it, because succeeded accepts no further transition and
// cancelled only accepts retry — and executor completions are fenced to the
// attempt they were produced for, so a stale attempt can never overwrite a
// newer committed attempt.
var jobProtocol = map[JobTransition]jobRule{
	SubmitJob:  {event: "job.queued", to: JobQueued},
	ClaimJob:   {event: "job.running", to: JobRunning, from: []string{JobQueued}},
	CancelJob:  {event: "job.cancelled", to: JobCancelled, from: []string{JobQueued, JobRunning}, noop: true, deny: "job has already finished"},
	RetryJob:   {event: "job.retried", to: JobQueued, from: []string{JobFailed, JobCancelled}, bump: true, deny: "only failed or cancelled jobs can retry"},
	RecoverJob: {event: "job.recovered", to: JobQueued, from: []string{JobRunning}},
	SucceedJob: {event: "job.succeeded", to: JobSucceeded, from: []string{JobRunning}, fence: true},
	FailJob:    {event: "job.failed", to: JobFailed, from: []string{JobRunning}, fence: true},
	AbortJob:   {event: "job.cancelled", to: JobCancelled, from: []string{JobRunning}, fence: true},
}

// JobFact carries one lifecycle transition and everything needed to commit
// it against the persistent state.
type JobFact struct {
	Transition JobTransition
	JobID      string    // job the fact applies to; unused by SubmitJob
	Draft      *Job      // SubmitJob only: id and fixed input of the job to create
	Attempt    int       // attempt the fact belongs to; checked by fenced transitions
	Error      string    // failure detail committed by FailJob
	Artifact   *Artifact // SucceedJob only: result stored and linked on commit
}

// CommitJob applies fact to the committed state, committing at most one
// lifecycle transition. It returns the resulting job and whether a new fact
// was committed. A fact the protocol rules out for the committed state is
// either reported as a conflict (operator requests) or silently dropped
// (executor facts already superseded by a newer committed fact); in both
// cases the committed job is returned unchanged.
func (s *State) CommitJob(fact JobFact) (Job, bool, error) {
	rule, ok := jobProtocol[fact.Transition]
	if !ok {
		return Job{}, false, fmt.Errorf("unknown job transition %q", fact.Transition)
	}
	if rule.from == nil {
		if fact.Draft == nil {
			return Job{}, false, fmt.Errorf("job transition %q requires a draft", fact.Transition)
		}
		j := *fact.Draft
		if _, ok := s.Jobs[j.ID]; ok {
			return Job{}, false, fmt.Errorf("job %s is already committed", j.ID)
		}
		now := time.Now().UTC()
		j.State = rule.to
		j.Attempt = 1
		j.Error = ""
		j.ArtifactID = ""
		j.CreatedAt = now
		j.UpdatedAt = now
		s.Jobs[j.ID] = j
		s.Emit(rule.event, j.ID)
		return j, true, nil
	}
	if fact.Draft != nil {
		return Job{}, false, fmt.Errorf("job transition %q takes no draft", fact.Transition)
	}
	j, ok := s.Jobs[fact.JobID]
	if !ok {
		return Job{}, false, Missing("job not found")
	}
	eligible := false
	for _, from := range rule.from {
		eligible = eligible || j.State == from
	}
	if eligible && rule.fence {
		eligible = j.Attempt == fact.Attempt
	}
	if !eligible {
		switch {
		case rule.noop && j.State == rule.to:
			return j, false, nil
		case rule.deny != "":
			return j, false, Conflict(rule.deny)
		default:
			return j, false, nil
		}
	}
	if fact.Transition == SucceedJob {
		if fact.Artifact == nil {
			return Job{}, false, fmt.Errorf("job transition %q requires an artifact", fact.Transition)
		}
		a := *fact.Artifact
		a.ID = ID("png", s.Sequence+1)
		a.JobID = j.ID
		s.Artifacts[a.ID] = a
		j.ArtifactID = a.ID
	}
	j.State = rule.to
	if rule.bump {
		j.Attempt++
		j.Error = ""
	}
	if fact.Transition == FailJob {
		j.Error = fact.Error
	}
	j.UpdatedAt = time.Now().UTC()
	s.Jobs[j.ID] = j
	s.Emit(rule.event, j.ID)
	return j, true, nil
}
