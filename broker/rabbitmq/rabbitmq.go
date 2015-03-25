package gocelery

import (
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/taoh/gocelery/broker"

	// ampq broker
	"github.com/streadway/amqp"
)

// RabbitMqBroker implements RabbitMq broker
type RabbitMqBroker struct {
	sync.Mutex
	amqpURL string

	connection      *amqp.Connection
	channel         *amqp.Channel
	resultsChannels map[string]*amqp.Channel
}

//
func init() {
	// register rabbitmq
	broker.Register("amqp", &RabbitMqBroker{})
	broker.Register("amqps", &RabbitMqBroker{})
}

func (b *RabbitMqBroker) String() string {
	return fmt.Sprintf("AMQP Broker [%s]", b.amqpURL)
}

// Connect to rabbitmq
func (b *RabbitMqBroker) Connect(uri string) error {
	b.amqpURL = uri
	log.Debugf("Dialing [%s]", uri)
	// dial the server
	conn, err := amqp.Dial(b.amqpURL)
	if err != nil {
		return err
	}

	// create the channel
	b.connection = conn
	b.channel, err = b.connection.Channel()
	if err != nil {
		return err
	}

	b.resultsChannels = make(map[string]*amqp.Channel)

	log.Debug("Connected to rabbitmq")
	//create exchanges
	// note that the exchange must be the same as celery to avoid fatal errors
	err = b.newExchange("celery", "direct", true, false)
	if err != nil {
		return err
	}
	if err = b.newExchange("celeryresults", "direct", true, false); err != nil {
		return err
	}
	if err = b.newExchange("celeryev", "topic", true, false); err != nil {
		return err
	}

	log.Debug("Created exchanges")

	// create and bind queues
	queueName := queueName()
	var arguments amqp.Table
	// queueExpires := viper.GetInt("queueExpires") //ARGV:
	// if queueExpires > 0 {
	// 	arguments = amqp.Table{"x-expires": queueExpires}
	// }
	if err = b.newQueue(queueName, true, false, arguments); err != nil {
		return err
	}
	log.Debug("Created Task Queue")
	// bind queue to exchange
	if err = b.channel.QueueBind(
		queueName, // queue name
		queueName, // routing key
		"celery",  // exchange name
		false,     // noWait
		nil,       // arguments
	); err != nil {
		return err
	}
	log.Debug("Queue is bound to exchange")

	return nil
}

// Close the broker and cleans up resources
func (b *RabbitMqBroker) Close() error {
	log.Debug("Closing broker: ", b)
	return b.connection.Close()
}

// GetTasks waits and fetches the tasks from queue
func (b *RabbitMqBroker) GetTasks() <-chan *broker.Message {
	msg := make(chan *broker.Message)
	go func() {
		// fetch messages
		log.Infof("Waiting for tasks at: %s", b.amqpURL)
		deliveries, err := b.channel.Consume(
			"celery",
			"",   // Consumer
			true, // AutoAck
			false, false, false, nil)
		if err != nil {
			log.Error("Failed to consume task messages: ", err)
			//TODO: deal with channel failure
			return
		}
		for delivery := range deliveries {
			log.Debug("Got a message!")
			msg <- &broker.Message{
				ContentType: delivery.ContentType,
				Body:        delivery.Body,
			}
		}
		close(msg) // close message after channel closed
		// fake tests
		// for i := 0; i < 3; i++ {
		// 	args := []int{i, 5}
		// 	newArgs := make([]interface{}, len(args))
		// 	for i, v := range args {
		// 		newArgs[i] = interface{}(v)
		// 	}
		// 	task := &Task{
		// 		Task: "test.add",
		// 		ID:   fmt.Sprintf("%d", i),
		// 		Args: newArgs,
		// 	}
		// 	body, _ := json.Marshal(task)
		//
		// 	msg <- &broker.Message{ContentType: "application/json", Body: body}
		// 	time.Sleep(1 * time.Second)
		// }
	}()
	return msg
}

func (b *RabbitMqBroker) channelForTask(name string) *amqp.Channel {
	b.Lock()
	defer b.Unlock()

	if channel, ok := b.resultsChannels[name]; ok {
		return channel
	}
	channel, err := b.connection.Channel()
	if err != nil {
		return nil
	}
	b.resultsChannels[name] = channel
	return channel
}

// GetTaskResult fetchs task result for the specified taskID
func (b *RabbitMqBroker) GetTaskResult(taskID string) <-chan *broker.Message {
	msg := make(chan *broker.Message)
	go func() {
		// fetch messages
		log.Debug("Waiting for Task Result Messages: ", taskID)
		channel := b.channelForTask(taskID)
		if channel == nil {
			log.Error("Cannot get channel for task")
			return
		}
		deliveries, err := channel.Consume(
			taskID,
			taskID, // Consumer tag
			false,  // AutoAck
			false, false, false, nil)

		// delete channel
		if err != nil {
			log.Error("Failed to consume task result messages: ", taskID, " error: ", err)
			//b.channel.QueueUnbind(taskID, taskID, "celeryresults", nil)
			// b.channel.Cancel(taskID, false)
			return
		}
		delivery := <-deliveries
		if delivery.Body == nil {
			log.Error("Got a task result message: ", taskID, " body: ", string(delivery.Body))
		}
		msg <- &broker.Message{
			ContentType: delivery.ContentType,
			Body:        delivery.Body,
		}
		channel.Close()

		b.Lock()
		delete(b.resultsChannels, taskID)
		b.Unlock()
		// delete queue
		//b.channel.QueueUnbind(taskID, taskID, "celeryresults", nil)
		// err = b.channel.Cancel(taskID, false)
		//log.Info("Deleting queue: ", taskID, " err: ", err)

	}()
	return msg
}

// PublishTask sends a task to queue
func (b *RabbitMqBroker) PublishTask(key string, message *broker.Message, ignoreResults bool) error {
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  message.ContentType,
		Body:         message.Body,
	}

	if !ignoreResults {
		log.Debug("Creating queues for Task:", key)
		// create task result queue
		var arguments amqp.Table
		// queueExpires := viper.GetInt("resultQueueExpires") //ARGV:
		// if queueExpires > 0 {
		// 	arguments = amqp.Table{"x-expires": queueExpires}
		// }
		if err := b.newQueue(key, true, true, arguments); err != nil {
			log.Error("Failed to create queue: ", err)
			return err
		}
		log.Debug("Created Task Result Queue")
		// bind queue to exchange
		queueName := key
		if err := b.channelForTask(key).QueueBind(
			queueName,       // queue name
			queueName,       // routing key
			"celeryresults", // exchange name
			false,           // noWait
			nil,             // arguments
		); err != nil {
			return err
		}
	} else {
		log.Debug("Task Result ignored")
	}
	log.Debug("Publishing Task to queue")
	return b.channel.Publish("celery", "celery", false, false, msg)
}

// PublishTaskResult sends task result back to task queue
func (b *RabbitMqBroker) PublishTaskResult(key string, message *broker.Message) error {
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  message.ContentType,
		Body:         message.Body,
	}
	log.Debug("Publishing Task Result:", key)
	return b.channel.Publish("celeryresults", key, false, false, msg)
}

// PublishTaskEvent sends task events back to event queue
func (b *RabbitMqBroker) PublishTaskEvent(key string, message *broker.Message) error {
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  message.ContentType,
		Body:         message.Body,
	}
	return b.channel.Publish("celeryev", key, false, false, msg)
}

func (b *RabbitMqBroker) newExchange(name string, exchangeType string, durable bool, autoDelete bool) error {
	err := b.channel.ExchangeDeclare(
		name,         // empty name exchange
		exchangeType, // direct or topic
		durable,      // durable true or false
		autoDelete,   // autoDelete
		false,        // internal
		false,        // noWait
		nil,
	)
	return err
}

func (b *RabbitMqBroker) newQueue(name string, durable bool, autoDelete bool, arguments amqp.Table) error {
	_, err := b.channel.QueueDeclare(
		name,       // queue name
		durable,    // durable
		autoDelete, // autoDelete
		false,      // exclusive
		false,      // noWait
		arguments,
	)
	return err
}

func queueName() string {
	return "celery"
}
