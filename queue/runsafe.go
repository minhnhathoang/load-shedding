package queue

import "log"

// runSafe runs fn, recovering from panics so a faulty task can't kill a worker.
// It replaces go-zero's threading.RunSafe to keep this package dependency-free.
func runSafe(fn func()) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("queue: recovered from panic in task: %v", p)
		}
	}()
	fn()
}
