package gocelery

import (
	"encoding/json"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-errors/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/taoh/gocelery/broker"

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

// Worker runs the worker command
func workerCmd(cmd *cobra.Command, args []string) {
	log.Debug("Inside Worker: ", viper.Get("BrokerUrl"))
	broker := broker.NewBroker(viper.GetString("BrokerUrl"))
	log.Debug("Got broker: ", broker)
	log.Debug("Worker is now running")

	// now loops to wait for messages
	workerEventPayload, _ := json.Marshal(NewWorkerEvent(WorkerOnline))
	broker.PublishTaskEvent(strings.Replace(WorkerOnline.RoutingKey(), "-", ".", -1),
		JSON, workerEventPayload)

	// start hearbeat
	startHeartbeat(broker)
	for {
		messages, err := broker.GetTasks()
		if err != nil {
			log.Error("Failed to get tasks: ", err)
			continue
		} else {
			for message := range messages {
				log.Info("Message type: ", message.ContentType)
				serializer, err := serializer.NewSerializer(message.ContentType)
				// convert message body to task
				if err == nil {
					var task Task
					err = serializer.Deserialize(message.Body, &task)
					if err == nil {

						// publish task received event
						taskEventPayload, _ := serializer.Serialize(NewTaskReceivedEvent(&task))
						broker.PublishTaskEvent(strings.Replace(TaskReceived.RoutingKey(), "-", ".", -1),
							message.ContentType, taskEventPayload)
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
								runTask(&task, broker, message.ContentType, serializer)
							}()
						} else {
							// execute the task in a worker immediately
							go runTask(&task, broker, message.ContentType, serializer)
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

	//TODO: how do we close broker..
	stopHeartbeat(broker)
	workerEventPayload, _ = json.Marshal(NewWorkerEvent(WorkerOffline))
	broker.PublishTaskEvent(strings.Replace(WorkerOffline.RoutingKey(), "-", ".", -1),
		JSON, workerEventPayload)
	broker.Close()
}

var ticker *time.Ticker

func startHeartbeat(broker broker.Broker) {
	ticker = time.NewTicker(time.Second * 2)
	go func() {
		for t := range ticker.C {
			log.Debug("Send heartbeat", t)
			// send heartbeat
			workerEventPayload, _ := json.Marshal(NewWorkerEvent(WorkerHeartbeat))
			broker.PublishTaskEvent(strings.Replace(WorkerHeartbeat.RoutingKey(), "-", ".", -1),
				JSON, workerEventPayload)
		}
	}()
}

func stopHeartbeat(broker broker.Broker) {
	ticker.Stop()
}

func runTask(task *Task, b broker.Broker, contentType string, serializer serializer.Serializer) error {
	if worker, ok := workerRegistery[task.Task]; ok {
		log.Debug("***** Working on task: ", task.Task)
		taskEventPayload, _ := serializer.Serialize(NewTaskStartedEvent(task))
		taskEventType := TaskStarted
		b.PublishTaskEvent(strings.Replace(TaskStarted.RoutingKey(), "-", ".", -1),
			contentType, taskEventPayload)
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
		b.PublishTaskResult(key, contentType, res)

		// send task completed event
		if taskEventPayload != nil {
			b.PublishTaskEvent(strings.Replace(taskEventType.RoutingKey(), "-", ".", -1),
				contentType, taskEventPayload)
		}
	} else {
		log.Errorf("Worker for task [%s] not found", task.Task)
	}
	return nil
}
