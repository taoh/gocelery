# GoCelery

a golang port of [Celery](http://www.celeryproject.org/) distributed task engine


## Installation

```
go get http://github.com/taoh/gocelery
```

## Example
[demo/main.go](https://github.com/taoh/gocelery/blob/master/demo/main.go)

```go
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
```
run the demo

```bash
go run demo/main.go worker -l debug
```

## Documentation
[Godoc] (http://godoc.org/github.com/taoh/gocelery)

## Copyright and license
[The MIT License] (https://github.com/taoh/gocelery/blob/master/LICENSE)
