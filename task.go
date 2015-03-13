package gocelery

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/docker/swarm/version"
)

const celeryTimeFormat = `"2006-01-02T15:04:05.999999"`

type celeryTime struct {
	time.Time
}

var null = []byte("null")

func (ct *celeryTime) UnmarshalJSON(data []byte) (err error) {
	if bytes.Equal(data, null) {
		return
	}
	t, err := time.Parse(celeryTimeFormat, string(data))
	if err == nil {
		*ct = celeryTime{t}
	}
	return
}

func (ct *celeryTime) MarshalJSON() (data []byte, err error) {
	if ct.IsZero() {
		return null, nil
	}
	return []byte(ct.UTC().Format(celeryTimeFormat)), nil
}

// Task represents the a single piece of work
type Task struct {
	Task    string                 `json:"task"`
	ID      string                 `json:"id"`
	Args    []interface{}          `json:"args,omitempty"`
	Kwargs  map[string]interface{} `json:"kwargs,omitempty"`
	Retries int                    `json:"retries,omitempty"`
	Eta     celeryTime             `json:"eta,omitempty"`
	Expires celeryTime             `json:"expires,omitempty"`
}

func (t Task) String() string {
	return fmt.Sprintf("ID: %s, Task: %s, Args: %s", t.ID, t.Task, t.Args)
}

// ResultStatus is the valid statuses for task executions
type ResultStatus string

// ResultStatus values
const (
	Pending ResultStatus = "PENDING"
	Started ResultStatus = "STARTED"
	Success ResultStatus = "SUCCESS"
	Retry   ResultStatus = "RETRY"
	Failure ResultStatus = "FAILURE"
	Revoked ResultStatus = "REVOKED"
)

// TaskResult is the result wrapper for task
type TaskResult struct {
	ID        string       `json:"task_id"`
	Result    interface{}  `json:"result"`
	Status    ResultStatus `json:"status"`
	TraceBack string       `json:"traceback"`
}

type EventType string

const (
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

func (eventType EventType) RoutingKey() string {
	return strings.Replace(string(eventType), "-", ".", -1)
}

type WorkerEvent struct {
	Type      EventType `json:"type"`
	Ident     string    `json:"sw_ident"`
	Ver       string    `json:"sw_ver"`
	Sys       string    `json:"sw_sys"`
	HostName  string    `json:"hostname"`
	Timestamp int64     `json:"timestamp"`
}

var hostname, _ = os.Hostname()
var pid = os.Getpid()
var ver = version.VERSION

const (
	identity = "gocelery"
	system   = "golang"
)

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

func NewTaskSucceedEvent(task *Task, taskResult *TaskResult, runtime time.Duration) map[string]interface{} {
	taskEvent := map[string]interface{}{
		"type":      TaskReceived,
		"uuid":      task.ID,
		"result":    taskResult.Result,
		"runtime":   runtime,
		"hostname":  hostname,
		"timestamp": time.Now().Unix(),
	}
	return taskEvent
}

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
