package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"os"
	"stardust_queue/jobs"
	"stardust_queue/queue"
)

func main() {
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		log.Fatal(err)
	}

	// register queue driver
	manage := queue.NewManage(queue.AddRedisClient(c))
	manage.RegisterQueues()

	// bind jobs
	manage.BindJob(jobs.TestSignature, new(jobs.TestJob))
	manage.BindJob(jobs.Test2Signature, new(jobs.Test2Job))

	worker := manage.RegisterWorker(queue.AddSleep(10))
	fmt.Printf("进程ID：%d", os.Getpid())

	worker.Daemon("Redis", "test")
}
