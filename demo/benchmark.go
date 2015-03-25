// +build ignore

package main

import (
	"flag"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/taoh/gocelery"
)

func main() {
	var count int
	flag.IntVar(&count, "count", 1000, "number of tasks to execute")
	flag.Parse()
	args := []interface{}{12, 32}

	worker := gocelery.New(&gocelery.Config{
		LogLevel: "info",
	})
	defer worker.Close()

	log.Info("Benchmarking ")
	var wg sync.WaitGroup
	wg.Add(count)
	start := time.Now()
	for i := 0; i < count; i++ {
		go func(i int) {
			taskResult, err := worker.Enqueue(
				"tasks.add", // task name
				args,        // arguments
				false,       // ignoreResults
			)
			if err == nil {
				<-taskResult
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	log.Infof("%d tasks completed in %f seconds", count, elapsed.Seconds())
}
