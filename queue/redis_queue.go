package queue

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"stardust_queue/queue/scripts"
	"time"
)

type RedisQueue struct {
	c          redis.Conn
	retryAfter int // Unit:seconds
}

func NewRedisQueue(con redis.Conn, retryAfter int) *RedisQueue {
	queue := &RedisQueue{c: con}
	queue.retryAfter = retryAfter

	return queue
}

func (this *RedisQueue) Size(queue ...string) (int64, error) {
	prefixed := this.getQueueName(queue[0])
	size, err := scripts.Scripts["size"].Do(this.c, queue, prefixed+":delayed", prefixed+":reserved")

	return size.(int64), err
}

func (this *RedisQueue) Push(job IJob, queueName ...string) (JobID, error) {
	if len(queueName) == 0 {
		return this.PushRaw(CreatePayload(job), "")
	} else {
		return this.PushRaw(CreatePayload(job), queueName[0])
	}
}

func (this *RedisQueue) PushRaw(payload Payload, queueName ...string) (JobID, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	if len(queueName) == 0 {
		_, err = this.c.Do("rpush", this.getQueueName(""), string(b))
	} else {
		_, err = this.c.Do("rpush", this.getQueueName(queueName[0]), string(b))
	}

	return payload.ID, err
}

func (this *RedisQueue) Later(job IJob, delay time.Time, queue ...string) (JobID, error) {
	return this.LaterRaw(CreatePayload(job), delay, queue...)
}

func (this *RedisQueue) LaterRaw(payload Payload, delay time.Time, queueName ...string) (JobID, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	if len(queueName) == 0 {
		_, err = this.c.Do("zadd", this.getQueueName("")+":delayed", delay.Unix(), string(b))
	} else {
		_, err = this.c.Do("zadd", this.getQueueName(queueName[0])+":delayed", delay.Unix(), string(b))
	}

	return payload.ID, err
}

func (this *RedisQueue) Pop(queue ...string) ([]byte, error) {
	prefixed := this.getQueueName(queue[0])

	err := this.migrate(prefixed)
	if err != nil {
		return nil, err
	}
	reserved, err := this.retrieveNextJob(prefixed)

	// if the next job is not nil,will get [unmodified job,reserved job]
	data := reserved.([]interface{})
	if data[1] != nil {
		return data[1].([]byte), nil
	}
	return nil, nil
}

func (this *RedisQueue) getQueueName(queue string) string {
	if queue == "" {
		return fmt.Sprintf("stardust_queue:%s", "default")
	} else {
		return fmt.Sprintf("stardust_queue:%s", queue)
	}
}

// Migrate any delayed or expired jobs onto the primary queue.
func (this *RedisQueue) migrate(queue string) (err error) {
	err = this.migrateExpiredJobs(queue+":delayed", queue)
	if this.retryAfter != 0 {
		err = this.migrateExpiredJobs(queue+":reserved", queue)
	}
	return err
}

func (this *RedisQueue) migrateExpiredJobs(fromQueue string, toQueue string) error {
	// 使用 zrangebyscore 和 zremrangebyrank 从 {queue}:delayed 队列中，把- inf 到 now 的任务拿出来。
	//用 rpush 的方式存入到默认 queue 中（后续就是放入到 {queue}:reserved ）
	_, err := scripts.Scripts["migrateExpiredJobs"].Do(this.c, fromQueue, toQueue, time.Now().Unix())
	return err
}

func (this *RedisQueue) retrieveNextJob(queue string) (interface{}, error) {
	//把默认队列中的任务 lpop 出来
	//将他的 attempts 次数 + 1
	//zadd 存入 {queue}:reserved 队列，score 为 now+retry_after
	reserved, err := scripts.Scripts["pop"].Do(this.c, queue, queue+":reserved", time.Now().Add(time.Duration(this.retryAfter)*time.Second).Unix())
	return reserved, err
}

// 删除已经执行完的队列
func (this *RedisQueue) DeleteReserved(queue string, bs []byte) error {
	_, err := this.c.Do("zrem", this.getQueueName(queue)+":reserved", string(bs))
	return err
}
