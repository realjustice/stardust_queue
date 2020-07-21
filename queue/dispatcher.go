package queue

import (
	"time"
)

type Dispatcher struct {
	M *Manage
}

func NewDispatcher(manage *Manage) *Dispatcher {
	return &Dispatcher{M: manage}
}

func (this *Dispatcher) DispatchToQueue(job IJob, queueName ...string) error {
	driver, ok := this.M.Connectors[job.GetOption().Connector]
	if !ok {
		// if does not set connector,use the default driver
		driver = this.M.Connectors["Redis"]
	}

	return pushCommandToQueue(driver, job, queueName...)
}

func pushCommandToQueue(queue IQueue, job IJob, queueName ...string) (err error) {
	if job.GetOption().Delay > 0 {
		_, err = queue.Later(job, time.Now().Add(time.Duration(job.GetOption().Delay)*time.Second), queueName...)
		return
	}
	if len(queueName) == 0 {
		_, err = queue.Push(job)
	} else {
		_, err = queue.Push(job, queueName[0])
	}

	return
}
