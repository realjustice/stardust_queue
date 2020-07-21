package jobs

import (
	"fmt"
	"stardust_queue/queue"
)

// 唯一标识
var Test2Signature = "test2"

type Test2Job struct {
	Name string `json:"name"`
	BaseJob
}

func NewTest2Job(name string) *TestJob {
	job := &TestJob{Name: name}
	job.BaseJob.Signature = Test2Signature
	return job
}

func (this *Test2Job) Execute() queue.IJob {
	fmt.Printf("my name is: %s", this.Name)
	return this
}
