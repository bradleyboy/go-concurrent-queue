package manager

import (
	"context"
	"sync"
	"time"
)

// Job is a description of a unit of work that needs to be done
type Job interface {
	ID() int
	Version() int
	SetWorkerID(id string)
	WorkerID() string
	SetExpires(t time.Time)
	Expires() time.Time
	SetComplete()
	IsComplete() bool
	Available() bool
}

// JobEnumerater describes how to iterate through Jobs
type JobEnumerater interface {
	Next() Job
}

// Handler does the actual work for a given job
type Handler interface {
	Handle(ctx context.Context, job Job) error
}

// Storer handles fetching and claming jobs from a datastore
type Storer interface {
	GetAvailableJobs(ctx context.Context) (JobEnumerater, error)
	ClaimJob(ctx context.Context, job Job, worker Worker) error
	FetchJob(ctx context.Context, job Job) (Job, error)
	CompleteJob(ctx context.Context, job Job) error
	FailJob(ctx context.Context, job Job) error
}

// Worker is an interface that provides a unique ID
type Worker interface {
	ID() string
}

// Waiter controls how the manager pauses between runs
type Waiter interface {
	Pause()
}

// Manager manages work using a store and a handler
type Manager struct {
	sync.Mutex
	id      string
	store   Storer
	handler Handler
	wait    Waiter
	stop    bool
}

// New creates a new manager
func New(id string, store Storer, handler Handler, wait Waiter) *Manager {
	return &Manager{
		id:      id,
		store:   store,
		handler: handler,
		wait:    wait,
		stop:    false,
	}
}

// ID is the unique ID for this work manager
func (m *Manager) ID() string {
	return m.id
}

func (m *Manager) pause() {
	m.wait.Pause()
}

// Stop this Manager once it has completed its current work
func (m *Manager) Stop() {
	m.Lock()
	defer m.Unlock()
	m.stop = true
}

func (m *Manager) isStopped() bool {
	m.Lock()
	defer m.Unlock()
	return m.stop
}

// Start an instance of the Manager
func (m *Manager) Start(ctx context.Context) {
	for {
		// If worker has been asked to stop, exit the loop
		if m.isStopped() {
			break
		}

		// Get a list of available jobs from the store
		col, err := m.store.GetAvailableJobs(ctx)

		// Either we found no jobs, or there was a problem. Either way pause and try again.
		if err != nil {
			m.pause()
			continue
		}

		for {
			job := col.Next()

			if job == nil {
				break
			}

			err := m.store.ClaimJob(ctx, job, m)

			// Someone else got to it before us, keep looking.
			if err != nil {
				continue
			}

			// Fetch the entire job from the store, ensuring this worker is still the worker assigned
			j, err := m.store.FetchJob(ctx, job)

			if err != nil || j.WorkerID() != m.id {
				continue
			}

			// Do the actual work
			err = m.handler.Handle(ctx, j)

			if err != nil {
				err = m.store.FailJob(ctx, j)
				if err != nil {
					// log or something
				}

				m.pause()
				break
			}

			// Mark job as complete
			err = m.store.CompleteJob(ctx, j)

			if err != nil {
				// log or something
			}

			// Just do one job per iteration
			break
		}
	}
}
