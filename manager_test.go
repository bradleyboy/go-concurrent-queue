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

type testHandler struct{}

// Handle the job with some random delay, and fail roughly 10% of the time
func (h *testHandler) Handle(ctx context.Context, job *Job) error {
	if job.IsAvailable() {
		return errors.New("Job no longer available")
	}

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
	Misses         int
	Returns        int
	WorkerCounts   map[string]int
	FinishedLabels []string
}

type testStore struct {
	sync.Mutex
	jobs  map[int]*Job
	stats *storeStats
}

func newStore() *testStore {
	stats := &storeStats{
		Complete:       0,
		Misses:         0,
		Returns:        0,
		WorkerCounts:   make(map[string]int),
		FinishedLabels: make([]string, 0),
	}

	return &testStore{
		stats: stats,
		jobs:  make(map[int]*Job),
	}
}

func (s *testStore) Add(ctx context.Context, job *Job) (*Job, error) {
	s.Lock()
	defer s.Unlock()

	s.jobs[job.ID()] = job

	return job, nil
}

func (s *testStore) Lease(ctx context.Context, count int, duration time.Duration, workerID string) ([]*Job, error) {
	s.Lock()
	defer s.Unlock()

	jobs := make([]*Job, 0)

	for _, job := range s.jobs {
		if job.IsAvailable() {
			job.Lease(duration, workerID)
			jobs = append(jobs, job)
		}

		if len(jobs) == count {
			break
		}
	}

	if len(jobs) == 0 {
		s.stats.Misses++
		return nil, errors.New("No jobs available")
	}

	return jobs, nil
}

func (s *testStore) Return(ctx context.Context, job *Job) error {
	s.Lock()
	defer s.Unlock()

	job.Unlease()
	s.stats.Returns++
	return nil
}

func (s *testStore) Done(ctx context.Context, job *Job) error {
	s.Lock()
	defer s.Unlock()

	job.Complete()

	s.stats.Complete++

	_, ok := s.stats.WorkerCounts[job.WorkerID()]

	if ok {
		s.stats.WorkerCounts[job.WorkerID()]++
	} else {
		s.stats.WorkerCounts[job.WorkerID()] = 1
	}

	s.stats.FinishedLabels = append(s.stats.FinishedLabels, fmt.Sprintf("job-%d", job.ID()))

	return nil
}

type testBackoff struct{}

func (b *testBackoff) Pause() {
	time.Sleep(10 * time.Millisecond)
}

var managerTests = []struct {
	totalJobs    int
	totalWorkers int
}{
	{1000, 10}, // normal-ish
	{100, 40},  // high contention
	{10, 1},    // single worker
}

func TestManager(t *testing.T) {
	for _, tt := range managerTests {

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)

		store := newStore()
		for i := 1; i <= tt.totalJobs; i++ {
			_, _ = store.Add(ctx, NewJob(i, time.Now()))
		}

		for i := 1; i <= tt.totalWorkers; i++ {
			a := New(fmt.Sprintf("worker-%d", i), store, &testHandler{}, &testBackoff{})

			go func() {
				a.Start(ctx)
			}()
		}

		fmt.Printf("Total Jobs: %d Total Workers: %d\n", tt.totalJobs, tt.totalWorkers)

		// A sort of shot-in-the-dark calc to estimate how long this should take
		d := int(math.Min(20000, float64((tt.totalJobs/tt.totalWorkers)*jobDelay+(tt.totalWorkers*100))))
		fmt.Printf("Waiting %dms...\n", d)
		time.Sleep(time.Duration(d) * time.Millisecond)

		cancel()

		fmt.Printf("Stats: %+v\n", struct {
			Complete  int
			Misses    int
			Returns   int
			Workloads map[string]int
		}{
			Complete:  store.stats.Complete,
			Misses:    store.stats.Misses,
			Returns:   store.stats.Returns,
			Workloads: store.stats.WorkerCounts,
		})

		require.Equal(t, tt.totalWorkers, len(store.stats.WorkerCounts))
		require.Equal(t, tt.totalJobs, store.stats.Complete)

		total := 0
		for _, count := range store.stats.WorkerCounts {
			// We want each worker to do roughly the same amount of work, allowing for +-20% skew
			expected := tt.totalJobs / tt.totalWorkers
			delta := math.Max(10, float64(expected/5))
			require.InDelta(t, expected, count, delta)
			total += count
		}

		// Check that we did exactly the amount of jobs, and that we did all the individual jobs
		require.Equal(t, tt.totalJobs, total, "total jobs completed as reported by worker counts")
		require.Equal(t, tt.totalJobs, len(store.stats.FinishedLabels))
		for i := 1; i <= tt.totalJobs; i++ {
			require.Contains(t, store.stats.FinishedLabels, fmt.Sprintf("job-%d", i))
		}
	}
}

func TestDelayedJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	store := newStore()
	_, _ = store.Add(ctx, NewJob(1, time.Now()))
	_, _ = store.Add(ctx, NewJob(2, time.Now().Add(time.Duration(5*time.Second))))

	for i := 1; i <= 2; i++ {
		a := New(fmt.Sprintf("worker-%d", i), store, &testHandler{}, &testBackoff{})

		go func() {
			a.Start(ctx)
		}()
	}

	time.Sleep(1 * time.Second)

	cancel()

	require.Equal(t, 1, store.stats.Complete)
	require.Equal(t, 1, len(store.stats.FinishedLabels))
	require.Contains(t, store.stats.FinishedLabels, "job-1")

	// Ensure job-2 never ran, since it was set farther in the future than the time we waited
	require.NotContains(t, store.stats.FinishedLabels, "job-2")
}
