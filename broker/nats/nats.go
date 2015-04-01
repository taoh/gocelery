package nats

import (
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/taoh/gocelery/broker"

	// nats broker
	"github.com/apcera/nats"
)

const (
	// TaskEventChannel for task event pubsub
	TaskEventChannel string = "gocelerytaskevent"
)

// Broker implements Nats broker
type Broker struct {
	sync.Mutex
	natsURL string

	connection *nats.EncodedConn
}

//
func init() {
	// register nats
	broker.Register("nats", &Broker{})
}

// Connect to rabbitmq
func (b *Broker) Connect(uri string) error {
	b.natsURL = uri
	log.Debugf("Dialing [%s]", uri)

	// dial the server
	conn, err := nats.Connect(b.natsURL)
	if err != nil {
		return err
	}

	// create the encoded connection
	b.connection, err = nats.NewEncodedConn(conn, "json")
	if err != nil {
		return err
	}

	log.Debug("Connected to gnatsd")

	return nil
}

// Close the broker and cleans up resources
func (b *Broker) Close() error {
	log.Debug("Closing broker: ", b.natsURL)
	b.connection.Close()
	return nil
}

// GetTasks waits and fetches the tasks from queue
func (b *Broker) GetTasks(queue string) <-chan *broker.Message {
	msg := make(chan *broker.Message)

	// fetch messages
	log.Infof("Waiting for tasks at: %s, queue: %s", b.natsURL, queue)
	b.connection.Subscribe(queue, func(m *broker.Message) {
		msg <- m
	})
	return msg
}

// GetTaskResult fetchs task result for the specified taskID
func (b *Broker) GetTaskResult(taskID string) <-chan *broker.Message {
	msg := make(chan *broker.Message)

	// fetch messages
	log.Debug("Waiting for Task Result Messages: ", taskID)
	subs, _ := b.connection.Subscribe(taskID, func(m *broker.Message) {
		log.Debug("Task Result message: ", string(m.Body))
		msg <- m
	})
	subs.AutoUnsubscribe(1) // only want 1 result.
	log.Debug("Subscribed to Task Result")
	return msg
}

// PublishTask sends a task to queue
func (b *Broker) PublishTask(queueName, key string, message *broker.Message, ignoreResults bool) error {
	err := b.connection.Publish(queueName, message)
	if err != nil {
		return err
	}

	log.Debug("Published Task to queue: ", key)
	return nil
}

// PublishTaskResult sends task result back to task queue
func (b *Broker) PublishTaskResult(key string, message *broker.Message) error {
	log.Debug("Publishing Task Result:", key)
	b.connection.Publish(key, message)
	return nil
}

// PublishTaskEvent sends task events back to event queue
func (b *Broker) PublishTaskEvent(key string, message *broker.Message) error {
	return b.connection.Publish(TaskEventChannel, message)
}
