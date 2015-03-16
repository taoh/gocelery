// +build ignore

package main

import (
	"fmt"
	"math/rand"
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

	// simulate the wait
	time.Sleep(time.Duration(rand.Int31n(3000)) * time.Millisecond)
	log.Debug("task.Args: ", task.Args, " Result: ", result)
	return
}

func main() {
	gocelery.RegisterWorker("tasks.add", &Adder{})
	// print all registered workers
	workers := gocelery.RegisteredWorkers()
	fmt.Println("Workers:")
	for _, worker := range workers {
		fmt.Printf("\t%s", worker)
	}

	// start executing
	gocelery.Execute()
}
