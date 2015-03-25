package gocelery

import (
	"fmt"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/robfig/cron"

	// import rabbitmq broker
	_ "github.com/taoh/gocelery/broker/rabbitmq"
)

// GoCelery creates an instance of entry
type GoCelery struct {
	config        *Config
	workerManager *workerManager
	cron          *cron.Cron
}

// New creates a GoCelery instance with given config
func New(config *Config) *GoCelery {
	if config.LogLevel == "" {
		config.LogLevel = "info"
	}
	if config.BrokerURL == "" {
		config.BrokerURL = "amqp://localhost"
	}
	gocelery := &GoCelery{
		config: config,
		workerManager: &workerManager{
			brokerURL: config.BrokerURL,
		},
		cron: cron.New(),
	}
	// set up log level
	setupLogLevel(config)

	// try connect to worker
	if gocelery.workerManager.Connect() != nil {
		panic(fmt.Sprintf("Failed to connect to broker: %s", config.BrokerURL))
	}
	// start cron work
	gocelery.cron.Start()
	return gocelery
}

// Close disconnects with broker and cleans up all resources used.
// Use a defer statement to make sure resources are closed
func (gocelery *GoCelery) Close() {
	// make sure we're closed
	gocelery.workerManager.Close()
	gocelery.cron.Stop()
	log.Info("gocelery stopped.")
}

// set up log level. default is error
func setupLogLevel(config *Config) {
	log.SetOutput(os.Stderr)
	level, err := log.ParseLevel(config.LogLevel)
	if err != nil {
		log.Warnf("Failed to set log level: %s. Use default: error", config.LogLevel)
		level = log.ErrorLevel
	}
	log.SetLevel(level)

	log.Debug("Log Level: ", level)
}

// Enqueue adds a task to queue to be executed immediately. If ignoreResult is true
// the function returns immediately with a nil channel returned. Otherwise, a result
// channel is returned so client can wait for the result.
func (gocelery *GoCelery) Enqueue(taskName string, args []interface{}, ignoreResult bool) (chan *TaskResult, error) {
	task, err := gocelery.workerManager.PublishTask(taskName, args, nil, time.Time{}, time.Time{}, ignoreResult)
	if err != nil {
		return nil, err
	}
	if ignoreResult {
		log.Debug("Task Result is ignored.")
		return nil, nil
	}
	taskResult := make(chan *TaskResult)
	go func() {
		log.Debug("Waiting for task result")
		taskResult <- gocelery.workerManager.GetTaskResult(task)
	}()
	return taskResult, nil
}

// EnqueueWithSchedule adds a task that is scheduled repeatedly.
// Schedule is specified in a string with cron format
func (gocelery *GoCelery) EnqueueWithSchedule(spec string, taskName string, args []interface{}) error {
	return gocelery.cron.AddFunc(spec, func() {
		log.Infof("Running scheduled task %s: %s", spec, taskName)
		gocelery.Enqueue(taskName, args, true)
	})
}

// StartWorkers start running the workers
func (gocelery *GoCelery) StartWorkers() {
	log.Info("gocelery started.")
	gocelery.workerManager.Start()
}
