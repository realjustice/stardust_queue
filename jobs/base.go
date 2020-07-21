package jobs

import (
	"stardust_queue/queue"
)

type BaseJob struct {
	Signature string `json:"signature"`
	op        queue.JobOption
}

func (this *BaseJob) Execute() queue.IJob {
	return this
}

func (this *BaseJob) GetOption() queue.JobOption {
	return this.op
}

func (this *BaseJob) SetOption(queueName string, delay int) queue.JobOption {
	op := queue.JobOption{QueueName: queueName, Delay: delay}
	this.op = op
	return this.op
}

func (this *BaseJob) SetDelay(delay int) {
	this.op.Delay = delay
}

func (this *BaseJob) SetQueueName(queueName ...string) {
	if len(queueName) == 0 {
		this.op.QueueName = queueName[0]
	}
}
