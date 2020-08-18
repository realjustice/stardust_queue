package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"os"
	"stardust_queue/jobs"
	"stardust_queue/queue"
	"time"
)

func main() {
	fmt.Printf("进程ID：%d", os.Getpid())
	pool := newPool()

	conn1 := pool.Get()
	go InitWorker(conn1)
	conn2 := pool.Get()
	go InitWorker(conn2)
	conn3 := pool.Get()
	go InitWorker(conn3)
	conn4 := pool.Get()
	go InitWorker(conn4)
	conn5 := pool.Get()
	go InitWorker(conn5)
	conn6 := pool.Get()
	go InitWorker(conn6)
	conn7 := pool.Get()
	go InitWorker(conn7)
	conn8 := pool.Get()
	go InitWorker(conn8)

	time.Sleep(10 * time.Hour)
}

func InitWorker(conn redis.Conn) {

	// register queue driver
	manage := queue.NewManage(queue.AddRedisClient(conn), queue.AddRetryAfter(30))
	manage.RegisterQueues()

	// bind jobs
	manage.BindJob(jobs.TestSignature, new(jobs.TestJob))
	manage.BindJob(jobs.Test2Signature, new(jobs.Test2Job))

	// you can init options in this way
	worker := manage.RegisterWorker(queue.AddSleep(1), queue.AddMaxTries(3))

	worker.Daemon("Redis", "test")
}

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     5,
		MaxActive:   10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}
