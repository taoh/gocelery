// +build ignore

package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"
	"github.com/taoh/gocelery"
)

func main() {
	var queues = flag.String("queue", "", "queue for sending the tasks to.")
	flag.Parse()

	log.Info("We can run the task and ignore result")
	i := 13
	j := 12
	args := []interface{}{i, j}

	worker := gocelery.New(&gocelery.Config{
		LogLevel: "debug",
		//BrokerURL: "nats://localhost:4222",
		BrokerURL: "redis://localhost:6379",
	})
	defer worker.Close()

	worker.Enqueue(
		"tasks.add", // task name
		args,        // arguments
		true,        // ignoreResults
	)

	log.Info("Task Executed.")

	var taskResult chan *gocelery.TaskResult
	if *queues == "" {
		taskResult, _ = worker.Enqueue(
			"tasks.add", // task name
			args,        // arguments
			false,       // ignoreResults
		)
	} else {
		taskResult, _ = worker.EnqueueInQueue(
			*queues,
			"tasks.add", // task name
			args,        // arguments
			false,       // ignoreResults
		)
	}

	tr := <-taskResult
	log.Infof("We can also run the task and return result: %d + %d = %d", i, j, int64(tr.Result.(float64)))
	log.Info("Task Executed.")
}
