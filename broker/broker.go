package broker

import (
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
)

//
// import (
// 	"encoding/json"
// 	"fmt"
// 	"strings"
// 	"time"
//
// 	log "github.com/Sirupsen/logrus"
// )
//
// type Deliveries chan *Task

// Message is the data got from broker
type Message struct {
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
func NewBroker(uri string) Broker {
	var scheme = strings.SplitN(uri, "://", 2)[0] // get scheme

	if broker, ok := brokerRegistery[scheme]; ok { // check if scheme is registered
		err := broker.Connect(uri)
		if err != nil {
			log.Error("Failed to connect to broker:", err)
			panic(fmt.Sprintf("Unable to connect to broker: [%s]", uri))
		}
		return broker
	}

	panic(fmt.Sprintf("Unknown broker [%s]", scheme)) // otherwise, panic and exits
}
