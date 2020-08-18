package queue

import (
	"time"
)

type IQueue interface {
	Size(queue ...string) (int64, error)                                       // Get the size of the queue.
	Push(job IJob, queue ...string) (JobID, error)                             // Push a new job onto the queue.
	PushRaw(payload Payload, queue ...string) (JobID, error)                   // Push a raw payload onto the queue.
	Later(job IJob, delay time.Time, queue ...string) (JobID, error)           // Push a new job onto the queue after a delay.
	LaterRaw(payload Payload, delay time.Time, queue ...string) (JobID, error) // Push a raw job onto the queue after a delay.
	Pop(queue ...string) ([]byte, error)                                       // Pop the next job off of the queue.
	DeleteReserved(queue string, bs []byte) error                              // Delete the reserved job.
}

type IJob interface {
	Execute() IJob                                   // execute the job
	SetOption(queueName string, delay int) JobOption // set job option
	GetOption() JobOption                            // get job option
	SetDelay(delay int)                              // set the job delay
	SetQueueName(queueName ...string)                // set the queue_name
}
