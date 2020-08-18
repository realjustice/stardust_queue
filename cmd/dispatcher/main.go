package main

import (
	"github.com/gomodule/redigo/redis"
	"log"
	"stardust_queue/jobs"
	"stardust_queue/queue"
	"time"
)

func main() {
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		log.Fatal(err)
	}

	// register queue driver
	manage := queue.NewManage(queue.AddRedisClient(c))
	manage.RegisterQueues()

	dispatcher := manage.RegisterDispatcher()
	for {
		job := jobs.NewTestJob("zsc")
		job.SetDelay(10)

		_ = dispatcher.DispatchToQueue(job, "test")
		time.Sleep(5 * time.Second)
	}
}
