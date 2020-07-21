package queue

import (
	uuid "github.com/satori/go.uuid"
)

// 生成序列化的Payload
func CreatePayload(job IJob) (p Payload) {
	p.Job = job
	p.ID = JobID(uuid.NewV4().String())

	return p
}
