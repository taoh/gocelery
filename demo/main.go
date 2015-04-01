// +build ignore

package main

import (
	"flag"
	"strings"

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
	//time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	//time.Sleep(1 * time.Second)
	log.Debug("task.Args: ", task.Args, " Result: ", result)
	return
}

func main() {

	var queues = flag.String("queues", "", "queues for running the workers. Use comma to separate multiple queues")
	flag.Parse()

	worker := gocelery.New(&gocelery.Config{
		LogLevel: "info",
		//BrokerURL: "nats://localhost:4222",
		BrokerURL: "redis://localhost:6379",
	})
	defer worker.Close()

	gocelery.RegisterWorker("tasks.add", &Adder{})
	// print all registered workers
	workers := gocelery.RegisteredWorkers()
	for _, worker := range workers {
		log.Debugf("Registered Worker: %s", worker)
	}

	if *queues == "" {
		// start executing
		worker.StartWorkers()
	} else {
		// start executing
		worker.StartWorkersWithQueues(strings.Split(*queues, ","))
	}

}
