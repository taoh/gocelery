package gocelery

import (
	"encoding/json"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-errors/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/taoh/gocelery/broker"
	"github.com/twinj/uuid"

	"github.com/taoh/gocelery/serializer"
)

// Worker is the definition of task execution
type Worker interface {
	Execute(*Task) (interface{}, error)
}

var workerRegistery = make(map[string]Worker)

// Constants
const (
	JSON string = "application/json"
)

// RegisterWorker registers the worker with given task name
func RegisterWorker(name string, worker Worker) {
	workerRegistery[name] = worker
}

// List all registered workers
func RegisteredWorkers() []string {
	keys := make([]string, 0, len(workerRegistery))
	for key := range workerRegistery {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// WorkerManager starts and stop worker jobs
type WorkerManager struct {
	broker broker.Broker

	ticker *time.Ticker // ticker for heartbeat
}

func (manager *WorkerManager) Connect() {
	log.Debug("Inside Worker: ", viper.Get("BrokerUrl"))
	b := broker.NewBroker(viper.GetString("BrokerUrl"))
	manager.broker = b
	log.Debug("Created broker: ", b)
}

// Worker runs the worker command
func (manager *WorkerManager) Start(cmd *cobra.Command, args []string) {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs       // listen to signals
		done <- true // send signals to done
	}()

	log.Debug("Worker is now running")

	// now loops to wait for messages
	manager.sendTaskEvent(WorkerOnline)

	// start hearbeat
	manager.startHeartbeat()

	for {
		select {
		case <-done: // if we have done
			log.Debug("CTRL+C Pressed, Exiting gracefully")
			manager.Stop()
			manager.Close()
			os.Exit(0)
		case message := <-manager.broker.GetTasks():
			log.Info("Message type: ", message.ContentType)
			serializer, err := serializer.NewSerializer(message.ContentType)
			// convert message body to task
			if err == nil {
				var task Task
				err = serializer.Deserialize(message.Body, &task)
				if err == nil {
					task.ContentType = message.ContentType // stores the content type for the task
					// publish task received event
					taskEventPayload, _ := serializer.Serialize(NewTaskReceivedEvent(&task))
					manager.broker.PublishTaskEvent(strings.Replace(TaskReceived.RoutingKey(), "-", ".", -1),
						&broker.Message{ContentType: message.ContentType, Body: taskEventPayload})
					log.Info("Processing task: ", task.Task)

					// check eta
					duration := time.Duration(0)
					if !task.Eta.IsZero() {
						duration = task.Eta.Sub(time.Now().UTC())
					}
					if duration > 0 {
						timer := time.NewTimer(duration) // wait until ready
						go func() {
							<-timer.C
							manager.runTask(&task)
						}()
					} else {
						// execute the task in a worker immediately
						go manager.runTask(&task)
					}
				} else {
					log.Error("Cannot deserialize content: ", message.ContentType, err)
				}
			} else {
				log.Error("Unknown message content type: ", message.ContentType)

			}

		}
	}
}

func (manager *WorkerManager) PublishTask(name string, args []interface{},
	kwargs map[string]interface{}, eta time.Time, expires time.Time) {
	task := &Task{
		Task:    name,
		Args:    args,
		Kwargs:  kwargs,
		Eta:     celeryTime{eta},
		Expires: celeryTime{expires},
	}
	task.ID = uuid.NewV4().String()

	res, _ := json.Marshal(task)
	message := &broker.Message{
		ContentType: JSON,
		Body:        res,
	}
	manager.broker.PublishTask("", message)
}

func (manager *WorkerManager) startHeartbeat() {
	ticker := time.NewTicker(time.Second * 2)
	manager.ticker = ticker
	go func() {
		for _ = range ticker.C {
			// log.Debug("Sent heartbeat at: ", t)
			// send heartbeat
			manager.sendTaskEvent(WorkerHeartbeat)
		}
	}()
}

func (manager *WorkerManager) sendTaskEvent(eventType EventType) {
	workerEventPayload, _ := json.Marshal(NewWorkerEvent(eventType))
	manager.broker.PublishTaskEvent(strings.Replace(eventType.RoutingKey(), "-", ".", -1),
		&broker.Message{ContentType: JSON, Body: workerEventPayload})
}

func (manager *WorkerManager) stopHeartbeat() {
	if manager.ticker != nil {
		manager.ticker.Stop()
	}
}

func (manager *WorkerManager) Stop() {
	manager.stopHeartbeat()
	manager.sendTaskEvent(WorkerOffline)
}

func (manager *WorkerManager) Close() {
	manager.broker.Close()
}

func (manager *WorkerManager) runTask(task *Task) error {
	if worker, ok := workerRegistery[task.Task]; ok {
		log.Debug("***** Working on task: ", task.Task)
		serializer, _ := serializer.NewSerializer(task.ContentType)
		taskEventPayload, _ := serializer.Serialize(NewTaskStartedEvent(task))
		taskEventType := TaskStarted
		manager.sendTaskEvent(TaskStarted)

		// create task result
		taskResult := &TaskResult{
			ID:     task.ID,
			Status: Started,
		}

		// check expiration
		if !task.Expires.IsZero() && task.Expires.Before(time.Now().UTC()) {
			// time expired, make the task Revoked
			log.Warn("Task has expired", task.Expires)
			taskResult.Status = Revoked
			taskEventType = TaskRevoked
		} else {
			start := time.Now()
			result, err := worker.Execute(task)
			elapsed := time.Since(start)
			if err != nil {
				log.Errorf("Failed to execute task [%s]: %s", task.Task, err)
				err = errors.Wrap(err, 1)
			}

			if err != nil {
				taskResult.Status = Failure
				taskResult.TraceBack = err.(*errors.Error).ErrorStack()
				taskEventPayload, _ = serializer.Serialize(NewTaskFailedEvent(task, taskResult, err))
				taskEventType = TaskFailed
			} else {
				taskResult.Status = Success
				taskResult.Result = result
				taskEventPayload, _ = serializer.Serialize(NewTaskSucceedEvent(task, taskResult, elapsed))
				taskEventType = TaskSucceeded
			}
		}
		res, _ := serializer.Serialize(taskResult)
		log.Debug("serialized output: ", len(res))
		key := strings.Replace(task.ID, "-", "", -1)
		manager.broker.PublishTaskResult(key, &broker.Message{ContentType: task.ContentType, Body: res})

		// send task completed event
		if taskEventPayload != nil {
			manager.sendTaskEvent(taskEventType)
		}
	} else {
		log.Errorf("Worker for task [%s] not found", task.Task)
	}
	return nil
}
