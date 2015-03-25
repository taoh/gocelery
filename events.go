package gocelery

import (
	"os"
	"strings"
	"time"

	"github.com/taoh/gocelery/version"
)

var hostname, _ = os.Hostname()
var pid = os.Getpid()
var ver = version.VERSION

const (
	identity = "gocelery"
	system   = "golang"
)

// EventType is enum of valid event types in celery
type EventType string

// Valid EventTypes
const (
	None            EventType = "None"
	WorkerOffline   EventType = "worker-offline"
	WorkerHeartbeat EventType = "worker-heartbeat"
	WorkerOnline    EventType = "worker-online"
	TaskRetried     EventType = "task-retried"
	TaskSucceeded   EventType = "task-succeeded"
	TaskStarted     EventType = "task-started"
	TaskReceived    EventType = "task-received"
	TaskFailed      EventType = "task-failed"
	TaskRevoked     EventType = "task-revoked"
)

// RoutingKey returns celery routing keys for events
func (eventType EventType) RoutingKey() string {
	return strings.Replace(string(eventType), "-", ".", -1)
}

// WorkerEvent implements the structure for worker related events
type WorkerEvent struct {
	Type      EventType `json:"type"`
	Ident     string    `json:"sw_ident"`
	Ver       string    `json:"sw_ver"`
	Sys       string    `json:"sw_sys"`
	HostName  string    `json:"hostname"`
	Timestamp int64     `json:"timestamp"`
}

// NewWorkerEvent creates new worker events
func NewWorkerEvent(eventType EventType) *WorkerEvent {
	return &WorkerEvent{
		Type:      eventType,
		Ident:     identity,
		Ver:       ver,
		Sys:       system,
		HostName:  hostname,
		Timestamp: time.Now().Unix(),
	}
}

//NewTaskReceivedEvent creates new event for task received
func NewTaskReceivedEvent(task *Task) map[string]interface{} {
	taskEvent := map[string]interface{}{
		"type":      TaskReceived,
		"uuid":      task.ID,
		"name":      task.Task,
		"args":      task.Args,
		"kwargs":    task.Kwargs,
		"retries":   task.Retries,
		"eta":       task.Eta,
		"hostname":  hostname,
		"timestamp": time.Now().Unix(),
	}
	return taskEvent
}

//NewTaskFailedEvent creates new event for task failed
func NewTaskFailedEvent(task *Task, taskResult *TaskResult, err error) map[string]interface{} {
	taskEvent := map[string]interface{}{
		"type":      TaskReceived,
		"uuid":      task.ID,
		"exception": err.Error(),
		"traceback": taskResult.TraceBack,
		"hostname":  hostname,
		"timestamp": time.Now().Unix(),
	}
	return taskEvent
}

//NewTaskSucceedEvent creates new event for task succeeded
func NewTaskSucceedEvent(task *Task, taskResult *TaskResult, runtime time.Duration) map[string]interface{} {
	taskEvent := map[string]interface{}{
		"type":      TaskReceived,
		"uuid":      task.ID,
		"result":    taskResult.Result,
		"runtime":   runtime.Seconds(),
		"hostname":  hostname,
		"timestamp": time.Now().Unix(),
	}
	return taskEvent
}

//NewTaskStartedEvent creates new event for task started
func NewTaskStartedEvent(task *Task) map[string]interface{} {
	taskEvent := map[string]interface{}{
		"type":      TaskReceived,
		"uuid":      task.ID,
		"pid":       pid,
		"hostname":  hostname,
		"timestamp": time.Now().Unix(),
	}
	return taskEvent
}
