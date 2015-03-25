// +build ignore

package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/taoh/gocelery"
)

func main() {
	log.Info("Scheduling tasks")

	done := make(chan struct{})

	worker := gocelery.New(&gocelery.Config{
		LogLevel: "debug",
	})
	defer worker.Close()

	i := 13
	j := 12
	taskArgs := []interface{}{i, j}
	if err := worker.EnqueueWithSchedule("*/5 * * * * *", "tasks.add", taskArgs); err != nil {
		log.Error("Failed to enqueue task: ", err)
	}

	log.Info("Scheduler started")
	<-done
}
