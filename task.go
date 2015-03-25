package gocelery

import (
	"bytes"
	"fmt"
	"time"
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
	Task        string                 `json:"task"`
	ID          string                 `json:"id"`
	Args        []interface{}          `json:"args,omitempty"`
	Kwargs      map[string]interface{} `json:"kwargs,omitempty"`
	Retries     int                    `json:"retries,omitempty"`
	Eta         celeryTime             `json:"eta,omitempty"`
	Expires     celeryTime             `json:"expires,omitempty"`
	ContentType string                 `json:"-"`
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
