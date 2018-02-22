package manager

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const jobDelay = 10

type testEnumerater struct {
	jobs  map[int]Job
	slice []Job
	index int
}

func newEnumerator(jobs map[int]Job) *testEnumerater {
	sl := make([]Job, 0, len(jobs))
	for _, j := range jobs {
		sl = append(sl, j)
	}

	return &testEnumerater{
		jobs:  jobs,
		index: -1,
		slice: sl,
	}
}

func (c *testEnumerater) Next() Job {
	c.index++

	if c.index >= len(c.slice) {
		return nil
	}

	return c.slice[c.index]
}

type testJob struct {
	id       int
	version  int
	label    string
	workerID string
	expires  time.Time
	complete bool
}

func (j *testJob) SetExpires(t time.Time) {
	j.expires = t
}

func (j *testJob) Expires() time.Time {
	return j.expires
}

func (j *testJob) IsComplete() bool {
	return j.complete
}

func (j *testJob) SetComplete() {
	j.complete = true
}

func (j *testJob) ID() int {
	return j.id
}

func (j *testJob) Version() int {
	return j.version
}

func (j *testJob) SetWorkerID(id string) {
	j.workerID = id
}

func (j *testJob) WorkerID() string {
	return j.workerID
}

func (j *testJob) Available() bool {
	return !j.IsComplete() && (j.WorkerID() == "" || j.Expires().Before(time.Now()))
}

type testHandler struct{}

// Handle the job with some random delay, and fail roughly 10% of the time
func (h *testHandler) Handle(ctx context.Context, job Job) error {
	r := rand.Intn(jobDelay)
	t := int(math.Floor(jobDelay / 20))

	if r <= t {
		return errors.New("I failed randomly")
	}

	time.Sleep(time.Duration(r) * time.Millisecond)
	return nil
}

type storeStats struct {
	Complete       int
	Collisions     int
	Fails          int
	WorkerCounts   map[string]int
	FinishedLabels []string
}

type testStore struct {
	sync.RWMutex
	jobs  map[int]Job
	stats *storeStats
}

func newStore(jobs map[int]Job) *testStore {
	stats := &storeStats{
		Complete:       0,
		Collisions:     0,
		Fails:          0,
		WorkerCounts:   make(map[string]int),
		FinishedLabels: make([]string, 0),
	}

	return &testStore{
		jobs:  jobs,
		stats: stats,
	}
}

func (s *testStore) GetAvailableJobs(ctx context.Context) (JobEnumerater, error) {
	s.RLock()
	defer s.RUnlock()

	jobs := make(map[int]Job)

	for id, job := range s.jobs {
		if job.Available() {
			jobs[id] = job
		}
	}

	return newEnumerator(jobs), nil
}

func (s *testStore) ClaimJob(ctx context.Context, job Job, worker Worker) error {
	s.Lock()
	defer s.Unlock()

	if job.Available() {
		job.SetWorkerID(worker.ID())
		job.SetExpires(time.Now().Add(5 * time.Second))
		return nil
	}

	s.stats.Collisions++
	return errors.New("Could not claim job")
}

func (s *testStore) FetchJob(ctx context.Context, job Job) (Job, error) {
	return job, nil
}

func (s *testStore) CompleteJob(ctx context.Context, job Job) error {
	s.Lock()
	defer s.Unlock()

	_, ok := s.stats.WorkerCounts[job.WorkerID()]

	if ok {
		s.stats.WorkerCounts[job.WorkerID()]++
	} else {
		s.stats.WorkerCounts[job.WorkerID()] = 1
	}

	job.SetComplete()

	s.stats.FinishedLabels = append(s.stats.FinishedLabels, fmt.Sprintf("job-%d", job.ID()))
	s.stats.Complete++
	return nil
}

func (s *testStore) FailJob(ctx context.Context, job Job) error {
	s.Lock()
	defer s.Unlock()

	job.SetExpires(time.Now().Add(5 * time.Millisecond))
	s.stats.Fails++
	return nil
}

type testBackoff struct{}

func (b *testBackoff) Pause() {
	time.Sleep(1 * time.Millisecond)
}

func TestManager(t *testing.T) {
	totalJobs := 1000
	totalWorkers := 10

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)

	jobs := make(map[int]Job)
	for i := 1; i <= totalJobs; i++ {
		jobs[i] = &testJob{id: i, version: 1, label: fmt.Sprintf("job-%d", i), expires: time.Now().Add(100 * time.Millisecond)}
	}

	store := newStore(jobs)

	for i := 1; i <= totalWorkers; i++ {
		a := New(fmt.Sprintf("worker-%d", i), store, &testHandler{}, &testBackoff{})

		go func() {
			a.Start(ctx)
		}()
	}

	fmt.Printf("Total Jobs: %d Total Workers: %d\n", totalJobs, totalWorkers)

	// A sort of shot-in-the-dark calc to estimate how long this should take
	d := int(math.Min(20000, float64((totalJobs/totalWorkers)*jobDelay+(totalWorkers*100))))
	fmt.Printf("Waiting %dms...\n", d)
	time.Sleep(time.Duration(d) * time.Millisecond)

	cancel()

	fmt.Printf("Stats: %+v\n", struct {
		Complete   int
		Collisions int
		Failures   int
		Workloads  map[string]int
	}{
		Complete:   store.stats.Complete,
		Collisions: store.stats.Collisions,
		Failures:   store.stats.Fails,
		Workloads:  store.stats.WorkerCounts,
	})

	require.Equal(t, totalWorkers, len(store.stats.WorkerCounts))
	require.Equal(t, totalJobs, store.stats.Complete)

	total := 0
	for _, count := range store.stats.WorkerCounts {
		// We want each worker to do roughly the same amount of work, allowing for +-10% skew
		require.InDelta(t, totalJobs/totalWorkers, count, float64(totalJobs/10))
		total += count
	}

	// Check that we did exactly the amount of jobs, and that we did all the individual jobs
	require.Equal(t, totalJobs, total)
	require.Equal(t, totalJobs, len(store.stats.FinishedLabels))
	for i := 1; i <= totalJobs; i++ {
		require.Contains(t, store.stats.FinishedLabels, fmt.Sprintf("job-%d", i))
	}
}
