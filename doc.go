// Package gocelery is a Golang implemenation of celery task queue.
// It allows you to enqueue a task and execute it using go runtime
// which brings efficiency and concurrency features
//
// gocelery is compatible with celery so that you can execute tasks
// enqueued by celery client or submit tasks to be executed by celery workers
//
// gocelery requires a broker and results backend. currently only
// rabbitmq is supported (although any AMQP broker should work but not
// tested)
//
// gocelery is shipped as a library so you can implement your own
// workers.
package gocelery
