package gocelery

import "sort"

// Worker is the definition of task execution
type Worker interface {
	Execute(*Task) (interface{}, error)
}

var workerRegistery = make(map[string]Worker)

// Constants
const (
	JSON string = "application/json"
)

// RegisterWorker registers the worker with given task name
func RegisterWorker(name string, worker Worker) {
	workerRegistery[name] = worker
}

// RegisteredWorkers List all registered workers
func RegisteredWorkers() []string {
	keys := make([]string, 0, len(workerRegistery))
	for key := range workerRegistery {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// IsWorkerRegistered checks if worker exists for the task name
func IsWorkerRegistered(name string) bool {
	_, ok := workerRegistery[name]
	return ok
}
