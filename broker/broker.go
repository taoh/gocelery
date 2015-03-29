package broker

import (
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

// Message is the data got from broker
type Message struct {
	Timestamp   time.Time
	ContentType string
	Body        []byte
}

// Broker implements the underlying broker for the task queue
type Broker interface {
	Connect(string) error
	GetTasks() <-chan *Message
	GetTaskResult(string) <-chan *Message
	PublishTask(string, *Message, bool) error
	PublishTaskResult(string, *Message) error
	PublishTaskEvent(string, *Message) error
	Close() error
}

var brokerRegistery = make(map[string]Broker)

// Register a broker based on its scheme
func Register(scheme string, b Broker) {
	brokerRegistery[scheme] = b
}

// NewBroker create a new broker based on the uri
func NewBroker(uri string) (Broker, error) {
	var scheme = strings.SplitN(uri, "://", 2)[0] // get scheme

	if broker, ok := brokerRegistery[scheme]; ok { // check if scheme is registered
		err := broker.Connect(uri)
		if err != nil {
			log.Error("Failed to connect to broker:", err)
			return nil, err
		}
		return broker, nil
	}

	return nil, fmt.Errorf("Unknown broker [%s]", scheme)
}
