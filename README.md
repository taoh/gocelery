# GoCelery

a golang port of [Celery](http://www.celeryproject.org/) distributed task engine. It supports executing and submitting tasks, and can interop with celery engine or celery python client.

[![Build Status](https://travis-ci.org/taoh/gocelery.svg?branch=master)](https://travis-ci.org/taoh/gocelery)

## Features

- Task Queues
- Task Event Reporting
- Supported Brokers
	* RabbitMQ
	* Redis
	* Nats

## Installation

```
go get http://github.com/taoh/gocelery
```

## Example
[demo/main.go](https://github.com/taoh/gocelery/blob/master/demo/main.go)

```go
package main

import (
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
	worker := gocelery.New(&gocelery.Config{
		LogLevel: "debug",
	})
	defer worker.Close()

	gocelery.RegisterWorker("tasks.add", &Adder{})
	// print all registered workers
	workers := gocelery.RegisteredWorkers()
	for _, worker := range workers {
		log.Debugf("Registered Worker: %s", worker)
	}

	// start executing
	worker.StartWorkers()
}
```
start the worker process

```bash
go run demo/main.go
```


[demo/client.go](https://github.com/taoh/gocelery/blob/master/demo/main.go)

```go
package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/taoh/gocelery"
)

func main() {
	log.SetLevel(log.InfoLevel)
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

	taskResult := worker.Enqueue(
		"tasks.add", // task name
		args,        // arguments
		false,       // ignoreResults
	)

	tr := <-taskResult
	log.Infof("We can also run the task and return result: %d + %d = %d", i, j, int64(tr.Result.(float64)))
	log.Info("Task Executed.")
}
```
start the client process

```bash
go run demo/client.go
```

## Documentation
[Godoc](http://godoc.org/github.com/taoh/gocelery)

## Copyright and license
[The MIT License](https://github.com/taoh/gocelery/blob/master/LICENSE)
