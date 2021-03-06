package queue

import (
	"github.com/gomodule/redigo/redis"
	"stardust_queue/configs"
)

type JobID string

type Manage struct {
	JobList     map[string]IJob
	Connectors  map[string]IQueue
	RedisClient redis.Conn
	rc          configs.Config
}

type manageOptionFunc func(*Manage)
type IManageOption interface {
	apply(*Manage)
}

func (f manageOptionFunc) apply(m *Manage) {
	f(m)
}

func (this *Manage) WithOptions(opts ...IManageOption) *Manage {
	c := this.clone()
	for _, opt := range opts {
		opt.apply(c)
	}
	return c
}

func AddRedisClient(c redis.Conn) IManageOption {
	return manageOptionFunc(func(m *Manage) {
		m.RedisClient = c
	})
}

func (this *Manage) clone() *Manage {
	copy := *this
	return &copy
}

// (Redis，Null) is currently supported only
func (this *Manage) RegisterQueues() {
	allowedConnectors := []string{"Null", "Redis"}
	for _, allowedConnector := range allowedConnectors {
		switch allowedConnector {
		case "Null":
			this.AddConnector("Null", NewNullQueue())
		case "Redis":
			this.AddConnector("Redis", NewRedisQueue(this.RedisClient, this.rc.RetryAfter, this.rc.QueueName))
		}
	}
}

// RegisterDispatcher
func (this *Manage) RegisterDispatcher() *Dispatcher {
	return NewDispatcher(this)
}

func (this *Manage) RegisterWorker(opts ...IWorkOption) *Worker {
	return NewWorker(this, opts...)
}

func NewManage(opts ...IManageOption) *Manage {
	m := &Manage{JobList: make(map[string]IJob), Connectors: make(map[string]IQueue)}

	return m.WithOptions(opts...)
}

func (this *Manage) AddConnector(driver string, connector IQueue) {
	this.Connectors[driver] = connector
}

// 绑定key,job
func (this *Manage) BindJob(key string, job IJob) {
	this.JobList[key] = job
}
