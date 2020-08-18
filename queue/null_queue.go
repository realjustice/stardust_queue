package queue

import (
	"time"
)

type NullQueue struct {
}

func NewNullQueue() *NullQueue {
	return &NullQueue{}
}

func (this *NullQueue) Size(queue ...string) (int64, error) {
	return 0, nil
}

func (this *NullQueue) Push(job IJob, queue ...string) (JobID, error) {
	return "", nil
}

func (this *NullQueue) PushRaw(payload Payload, queue ...string) (JobID, error) {
	return "", nil
}

func (this *NullQueue) Later(job IJob, delay time.Time, queue ...string) (JobID, error) {
	return "", nil
}

func (this *NullQueue) LaterRaw(payload Payload, delay time.Time, queue ...string) (JobID, error) {
	return "", nil
}

func (this *NullQueue) Pop(queue ...string) ([]byte, error) {
	return nil, nil
}

func (this *NullQueue) DeleteReserved(queue string, bs []byte) error {
	return nil
}
