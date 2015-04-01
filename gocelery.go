package gocelery

import (
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/robfig/cron"

	// import nats broker
	_ "github.com/taoh/gocelery/broker/nats"
	// import rabbitmq broker
	_ "github.com/taoh/gocelery/broker/rabbitmq"
	"github.com/twinj/uuid"
)

const (
	// DefaultQueue is the default task queue name
	DefaultQueue = "celery"
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

// EnqueueInQueue adds a task to queue to be executed immediately. If ignoreResult is true
// the function returns immediately with a nil channel returned. Otherwise, a result
// channel is returned so client can wait for the result.
func (gocelery *GoCelery) EnqueueInQueue(queueName string, taskName string, args []interface{}, ignoreResult bool) (chan *TaskResult, error) {
	log.Debugf("Enqueuing [%s] in queue [%s]", taskName, queueName)
	task := &Task{
		Task:    taskName,
		Args:    args,
		Kwargs:  nil,
		Eta:     celeryTime{time.Time{}},
		Expires: celeryTime{time.Time{}},
	}
	task.ID = uuid.NewV4().String()
	taskResult := make(chan *TaskResult)

	// make sure we subscribe to task result before we submit task
	// to avoid the problem that task may finish execution before we even subscribe
	// to result
	var wg sync.WaitGroup
	wg.Add(1)
	if !ignoreResult {
		log.Debug("Waiting for Task Result: ", task.ID)
		taskResult = gocelery.workerManager.GetTaskResult(task)
		wg.Done()
	} else {
		wg.Done()
	}

	wg.Wait()
	log.Debug("Publishing task: ", task.ID)
	task, err := gocelery.workerManager.PublishTask(queueName, task, ignoreResult)
	if err != nil {
		return nil, err
	}
	if ignoreResult {
		log.Debug("Task Result is ignored.")
		return nil, nil
	}

	return taskResult, nil
}

// Enqueue adds a task to queue to be executed immediately. If ignoreResult is true
// the function returns immediately with a nil channel returned. Otherwise, a result
// channel is returned so client can wait for the result.
func (gocelery *GoCelery) Enqueue(taskName string, args []interface{}, ignoreResult bool) (chan *TaskResult, error) {
	return gocelery.EnqueueInQueue(DefaultQueue, taskName, args, ignoreResult)
}

// EnqueueInQueueWithSchedule adds a task that is scheduled repeatedly.
// Schedule is specified in a string with cron format
func (gocelery *GoCelery) EnqueueInQueueWithSchedule(spec string, queueName string, taskName string, args []interface{}) error {
	return gocelery.cron.AddFunc(spec, func() {
		log.Infof("Running scheduled task %s: %s", spec, taskName)
		gocelery.EnqueueInQueue(queueName, taskName, args, true)
	})
}

// EnqueueWithSchedule adds a task that is scheduled repeatedly.
// Schedule is specified in a string with cron format
func (gocelery *GoCelery) EnqueueWithSchedule(spec string, queueName string, taskName string, args []interface{}) error {
	return gocelery.EnqueueInQueueWithSchedule(spec, DefaultQueue, taskName, args)
}

// StartWorkersWithQueues start running the workers
func (gocelery *GoCelery) StartWorkersWithQueues(queues []string) {
	log.Info("gocelery worker started with queues %s", queues)
	gocelery.workerManager.Start(queues)
}

// StartWorkers start running the workers with default queue
func (gocelery *GoCelery) StartWorkers() {
	gocelery.StartWorkersWithQueues([]string{DefaultQueue})
}
