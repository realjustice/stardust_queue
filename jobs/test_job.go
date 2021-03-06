package jobs

import (
	"fmt"
	"stardust_queue/queue"
)

// 唯一标识
var TestSignature = "test"

type TestJob struct {
	Name string `json:"name"`
	BaseJob
}

func NewTestJob(name string) *TestJob {
	job := &TestJob{Name: name}
	job.BaseJob.Signature = TestSignature

	return job
}

func (this *TestJob) Execute() queue.IJob {
	fmt.Printf("my name is: %s", this.Name)
	return this
}
