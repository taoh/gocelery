package gocelery

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-errors/errors"
	"github.com/taoh/gocelery/broker"
	"github.com/taoh/gocelery/serializer"
	"github.com/twinj/uuid"
)

// WorkerManager starts and stop worker jobs
type workerManager struct {
	brokerURL string
	broker    broker.Broker
	ticker    *time.Ticker // ticker for heartbeat
}

// Connect to broker. Returns an error if connection fails.
func (manager *workerManager) Connect() error {
	broker, err := broker.NewBroker(manager.brokerURL)
	if err != nil {
		log.Fatal("Failed to connect to broker: ", err)
		return err
	}

	log.Debug("Connected to broker: ", manager.brokerURL)
	manager.broker = broker
	return nil
}

// Start worker runs the worker command
func (manager *workerManager) Start() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs // listen to signals
		manager.Stop()
		manager.Close()
		done <- true // send signals to done
	}()

	log.Debug("Worker is now running")

	// now loops to wait for messages
	manager.sendWorkerEvent(WorkerOnline)

	// start hearbeat
	manager.startHeartbeat()

	// start getting tasks
	for message := range manager.broker.GetTasks() {
		log.Info("Message type: ", message.ContentType)
		go func(message *broker.Message) {
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
						manager.runTask(&task)
					}
				}
			} else {
				// send errors to server
				log.Error("Cannot deserialize message:", err)
			}
		}(message)
	}
	log.Debug("message channel closed.")
}

// PublishTask sends a task to task queue as a client
func (manager *workerManager) PublishTask(name string, args []interface{},
	kwargs map[string]interface{}, eta time.Time, expires time.Time, ignoreResult bool) *Task {
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
	manager.broker.PublishTask(task.ID, message, ignoreResult)
	// return the task object
	return task
}

// GetTaskResult listens to celery and returns the task result for given task
func (manager *workerManager) GetTaskResult(task *Task) *TaskResult {
	// listen to queue
	message := <-manager.broker.GetTaskResult(task.ID)
	serializer, _ := serializer.NewSerializer(message.ContentType)
	// convert message body to task
	var taskResult TaskResult
	_ = serializer.Deserialize(message.Body, &taskResult)
	return &taskResult
	// taskResult := &TaskResult{
	// 	ID:     task.ID,
	// 	Result: 5,
	// 	Status: Success,
	// }
}

func (manager *workerManager) startHeartbeat() {
	ticker := time.NewTicker(time.Second * 2)
	manager.ticker = ticker
	go func() {
		for _ = range ticker.C {
			// log.Debug("Sent heartbeat at: ", t)
			// send heartbeat
			manager.sendWorkerEvent(WorkerHeartbeat)
		}
	}()
}

func (manager *workerManager) sendTaskEvent(eventType EventType, payload []byte) {
	manager.broker.PublishTaskEvent(strings.Replace(eventType.RoutingKey(), "-", ".", -1),
		&broker.Message{ContentType: JSON, Body: payload})
}

// Send Worker Events
func (manager *workerManager) sendWorkerEvent(eventType EventType) {
	workerEventPayload, _ := json.Marshal(NewWorkerEvent(eventType))
	manager.broker.PublishTaskEvent(strings.Replace(eventType.RoutingKey(), "-", ".", -1),
		&broker.Message{ContentType: JSON, Body: workerEventPayload})
}

func (manager *workerManager) stopHeartbeat() {
	if manager.ticker != nil {
		manager.ticker.Stop()
	}
}

// Stop the worker
func (manager *workerManager) Stop() {
	manager.stopHeartbeat()
	manager.sendWorkerEvent(WorkerOffline)
}

// Close the worker
func (manager *workerManager) Close() {
	manager.broker.Close()
}

func (manager *workerManager) runTask(task *Task) (*TaskResult, error) {
	// create task result
	taskResult := &TaskResult{
		ID:     task.ID,
		Status: Started,
	}

	taskEventType := None
	serializer, _ := serializer.NewSerializer(task.ContentType)
	var taskError error
	var taskEventPayload []byte

	if worker, ok := workerRegistery[task.Task]; ok {
		log.Debug("Working on task: ", task.Task)

		taskEventPayload, _ = serializer.Serialize(NewTaskStartedEvent(task))
		taskEventType = TaskStarted
		manager.sendTaskEvent(TaskStarted, taskEventPayload)

		// check expiration
		if !task.Expires.IsZero() && task.Expires.Before(time.Now().UTC()) {
			// time expired, make the task Revoked
			log.Warn("Task has expired", task.Expires)
			taskResult.Status = Revoked
			taskError = errors.New("Task has expired")
			taskEventPayload, _ = serializer.Serialize(NewTaskFailedEvent(task, taskResult, taskError))
			taskEventType = TaskRevoked
		} else {
			start := time.Now()
			result, err := worker.Execute(task)
			elapsed := time.Since(start)
			if err != nil {
				log.Errorf("Failed to execute task [%s]: %s", task.Task, err)
				err = errors.Wrap(err, 1)
			} else {
				log.Debug("Executed task in ", elapsed.Seconds()) //TODO: doesn't work??
			}

			if err != nil {
				taskError = err
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
	} else {
		taskErrorMessage := fmt.Sprintf("Worker for task [%s] not found", task.Task)
		taskError = errors.New(taskErrorMessage)
		taskResult.Status = Failure
		taskResult.TraceBack = taskError.(*errors.Error).ErrorStack()
		taskEventPayload, _ = serializer.Serialize(NewTaskFailedEvent(task, taskResult, taskError))
		taskEventType = TaskFailed
	}

	res, _ := serializer.Serialize(taskResult)
	//key := strings.Replace(task.ID, "-", "", -1)
	manager.broker.PublishTaskResult(task.ID, &broker.Message{ContentType: task.ContentType, Body: res})

	// send task completed event
	if taskEventType != None {
		manager.sendTaskEvent(taskEventType, taskEventPayload)
	}

	return taskResult, nil
}
