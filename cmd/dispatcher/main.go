package main

import (
	"github.com/gomodule/redigo/redis"
	"log"
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

	dispatcher := manage.RegisterDispatcher()
	job := jobs.NewTestJob("zsc")
	job.SetDelay(60)

	_ = dispatcher.DispatchToQueue(job, "test")
}
