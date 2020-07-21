package queue

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"
)

type Worker struct {
	M          *Manage
	ShouldQuit bool
	Paused     bool
	option     WorkOption
}

func NewWorker(manage *Manage, opts ...IWorkOption) *Worker {
	w := &Worker{M: manage}
	return w.WithOptions(opts...)
}

type WorkOption struct {
	Delay    time.Time
	Memory   int64 // unit:kb
	Sleep    int   // will sleep a few moment in every loop
	MaxTries int
}

type IWorkOption interface {
	apply(*Worker)
}

type workOptionFunc func(*Worker)

func (f workOptionFunc) apply(log *Worker) {
	f(log)
}

func AddSleep(sleep int) IWorkOption {
	return workOptionFunc(func(w *Worker) {
		w.option.Sleep = sleep
	})
}

func (this *Worker) WithOptions(opts ...IWorkOption) *Worker {
	c := this.clone()
	for _, opt := range opts {
		opt.apply(c)
	}
	return c
}

func (this *Worker) clone() *Worker {
	copy := *this
	return &copy
}

// connector contract.IConnector, queue string, options Options
func (this *Worker) Daemon(connectorName string, queue string) {
	this.listenForSignals()
	for {
		// First, we will attempt to get the next job off of the queue. We will also
		// register the timeout handler and reset the alarm for this job so it is
		// not stuck in a frozen state forever. Then, we can fire off this job.
		driver, ok := this.M.Connectors[connectorName]
		if !ok {
			log.Fatal("undefined driver")
		}

		payload, err := this.getNextJob(driver, queue)
		if err != nil {
			log.Println(err)
		}

		if payload != nil {
			err := this.runJob(driver, payload)
			if err != nil {
				log.Println(err)
			}
		} else {
			time.Sleep(time.Duration(this.option.Sleep) * time.Second)
		}

		// Finally, we will check to see if we have exceeded our memory limits or if
		// the queue should restart based on other indications. If so, we'll stop
		// this worker and let whatever is "monitoring" it restart the process.

	}
}

func (this *Worker) getNextJob(driver IQueue, queue string) ([]byte, error) {
	return driver.Pop(queue)
}

func (this *Worker) runJob(queue IQueue, bs []byte) error {
	// raiseBeforeJobEvent

	// fire the job
	err := this.fire(bs)
	if err != nil {
		return err
	}
	// raiseAfterJobEvent

	return nil
}

// Fire the job.
func (this *Worker) fire(bs []byte) error {
	payloadMap := make(map[string]interface{})
	if err := json.Unmarshal(bs, &payloadMap); err != nil {
		return err
	}
	signature, ok := payloadMap["job"].(map[string]interface{})["signature"]
	if !ok {
		return errors.New("couldn't get the signature！")
	}

	job, ok := this.M.JobList[signature.(string)]
	if !ok {
		return errors.New("couldn't find the job！")
	}

	// 反射后执行
	jobInstance := reflect.New(reflect.TypeOf(job).Elem()).Interface().(IJob)
	var payload Payload
	payload.Job = jobInstance

	err := json.Unmarshal(bs, &payload)
	if err != nil {
		return err
	}
	payload.Job.Execute()
	return nil
}

func (this *Worker) listenForSignals() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGCONT)
	go func() {
		for s := range c {
			switch s {
			case syscall.SIGTERM:
				this.ShouldQuit = true
			case syscall.SIGUSR2:
				this.Paused = true
			case syscall.SIGCONT:
				this.Paused = false
			default:
				// nothing to do
			}
		}
	}()
}
