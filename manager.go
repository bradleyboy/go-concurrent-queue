package manager

import (
	"context"
	"sync"
	"time"
)

// Job is a description of a unit of work that needs to be done
type Job struct {
	sync.Mutex
	id       int
	workerID string
	begins   time.Time
	expires  time.Time
	complete bool
}

// ID returns the unique identifier for this job
func (j *Job) ID() int {
	return j.id
}

// IsAvailable returns whether the job is eligible for processing by a new worker
func (j *Job) IsAvailable() bool {
	j.Lock()
	defer j.Unlock()

	return !j.complete && j.expires.Before(time.Now()) && j.begins.Before(time.Now())
}

// WorkerID returns the assigned worker
func (j *Job) WorkerID() string {
	j.Lock()
	defer j.Unlock()
	return j.workerID
}

// Lease checks out the job for processing for a specific time, for a specific worker
func (j *Job) Lease(d time.Duration, workerID string) {
	j.Lock()
	defer j.Unlock()
	j.expires = time.Now().Add(d)
	j.workerID = workerID
}

// Complete marks the job as done
func (j *Job) Complete() {
	j.Lock()
	defer j.Unlock()
	j.complete = true
}

// Handler does the actual work for a given job
type Handler interface {
	Handle(ctx context.Context, job *Job) error
}

// Storer handles fetching and claming jobs from a datastore
type Storer interface {
	// Lease checks out `count` jobs for a given duration, assigning it to the workerID
	Lease(ctx context.Context, count int, duration time.Duration, workerID string) ([]*Job, error)
	// Return gives a job back ot the queue, signaling it should be retried
	Return(ctx context.Context, job *Job) error
	// Done marks the job complete, no further processing should occur
	Done(ctx context.Context, job *Job) error
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

		jobs, err := m.store.Lease(ctx, 1, time.Duration(10*time.Minute), m.id)
		// Either we found no jobs, or there was a problem. Either way pause and try again.
		if err != nil {
			m.pause()
			continue
		}

		for _, job := range jobs {
			// Do the actual work
			err = m.handler.Handle(ctx, job)

			if err != nil {
				err = m.store.Return(ctx, job)
				if err != nil {
					// log or something
				}

				m.pause()
				break
			}

			// Mark job as complete
			err = m.store.Done(ctx, job)

			if err != nil {
				// log or something
			}

			// Just do one job per iteration
			break
		}
	}
}
