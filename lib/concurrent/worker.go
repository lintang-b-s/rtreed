package concurrent

type Work func()

type WorkQueue chan Work

func NewWorkerQueue(numWorkers int) WorkQueue {
	queue := make(WorkQueue)
	d := make(dispatcher, numWorkers)
	go d.dispatch(queue)
	return queue
}

type dispatcher chan chan Work
type worker chan Work

func (d dispatcher) dispatch(queue WorkQueue) {

	for i := 0; i < cap(d); i++ {
		// start worker
		w := make(worker)
		go w.work(d)
	}

	go func() {
		for work := range queue {
			// new job
			go func(work Work) {
				worker := <-d
				worker <- work
			}(work)
		}

		// queue closed
		for i := 0; i < cap(d); i++ {
			w := <-d
			close(w)
		}
	}()
}

func (w worker) work(d dispatcher) {
	d <- w

	go w.wait(d)
}

func (w worker) wait(d dispatcher) {
	for work := range w {
		
		if work == nil {
			panic("nil work received")
		}
		// run the work function
		work()

		d <- w
	}
}
