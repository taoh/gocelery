// +build ignore

package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/taoh/gocelery"
)

func main() {
	log.Info("We can run the task and ignore result")
	i := 13
	j := 12
	args := []interface{}{i, j}

	worker := gocelery.New(&gocelery.Config{
		LogLevel: "debug",
	})
	defer worker.Close()

	worker.Enqueue(
		"tasks.add", // task name
		args,        // arguments
		true,        // ignoreResults
	)

	log.Info("Task Executed.")

	taskResult, _ := worker.Enqueue(
		"tasks.add", // task name
		args,        // arguments
		false,       // ignoreResults
	)

	tr := <-taskResult
	log.Infof("We can also run the task and return result: %d + %d = %d", i, j, int64(tr.Result.(float64)))
	log.Info("Task Executed.")
}
