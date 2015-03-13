package main

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/taoh/gocelery"
)

// Adder worker
type Adder struct{}

// Execute an addition
func (a *Adder) Execute(task *gocelery.Task) (result interface{}, err error) {
	sum := float64(0)
	for _, arg := range task.Args {
		switch arg.(type) {
		case int64:
			sum += (float64)(arg.(int64))
		case float64:
			sum += arg.(float64)
		}
	}
	result = sum
	log.Debug("task.Args: ", task.Args, " Result: ", result)
	time.Sleep(2 * time.Second)
	return
}

func main() {
	gocelery.RegisterWorker("tasks.add", &Adder{})
	gocelery.Execute()
}
