package util

import (
	"sync"

	"github.com/lintang-b-s/lbs/lib/index"
)

type JobI interface {
	index.SpatialData | index.Point
}

type Job[T JobI] struct {
	ID      int
	JobItem T
}

// ngasal wkwk
type WorkerPool[T JobI, G any] struct {
	numWorkers int
	JobQueue   chan T
	results    chan G
	wg         sync.WaitGroup
}

type JobFunc[T JobI, G any] func(job T) G

func NewWorkerPool[T JobI, G any](numWorkers, jobQueueSize int) *WorkerPool[T, G] {
	return &WorkerPool[T, G]{
		numWorkers: numWorkers,
		JobQueue:   make(chan T, jobQueueSize),
		results:    make(chan G, jobQueueSize),
	}
}

func (wp *WorkerPool[JobI, G]) worker(id int, jobFunc JobFunc[JobI, G]) {
	defer wp.wg.Done()
	for job := range wp.JobQueue {
		res := jobFunc(job)
		wp.results <- res
	}
}

func (wp *WorkerPool[JobI, G]) Start(jobFunc JobFunc[JobI, G]) {
	for i := 1; i <= wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i, jobFunc)
	}
}

func (wp *WorkerPool[JobI, G]) Wait() {
	wp.wg.Wait()
	close(wp.results)
}

func (wp *WorkerPool[JobI, G]) AddJob(job JobI) {
	wp.JobQueue <- job
}

func (wp *WorkerPool[JobI, G]) CollectResults() chan G {
	return wp.results
}
