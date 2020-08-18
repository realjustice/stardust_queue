package queue

import (
	"encoding/json"
	"errors"
	"fmt"
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

type WorkOption struct {
	Sleep    int // will sleep a few moment in every loop
	MaxTries int // max tries
}

func NewWorker(manage *Manage, opts ...IWorkOption) *Worker {
	w := &Worker{M: manage}
	return w.WithOptions(opts...)
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

func AddMaxTries(maxTries int) IWorkOption {
	return workOptionFunc(func(w *Worker) {
		w.option.MaxTries = maxTries
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
		if this.Paused {
			continue
		}
		driver, ok := this.M.Connectors.Load(connectorName)
		if !ok {
			log.Fatal("undefined driver")
		}

		payload, err := this.getNextJob(driver.(IQueue), queue)
		if err != nil {
			log.Println(err)
		}

		if payload != nil {
			err := this.runJob(driver.(IQueue), queue, payload)
			if err != nil {
				log.Println(err)
			}
		} else {
			time.Sleep(time.Duration(this.option.Sleep) * time.Second)
		}

		this.stopIfNecessary()
	}
}

func (this *Worker) getNextJob(driver IQueue, queue string) ([]byte, error) {
	return driver.Pop(queue)
}

func (this *Worker) runJob(queue IQueue, queueName string, bs []byte) error {
	// raiseBeforeJobEvent

	// markJobAsFailedIfAlreadyExceedsMaxAttempts

	// fire the job
	payload, err := this.initJob(bs)
	if err != nil {
		return err
	}

	err = this.markJobAsFailedIfAlreadyExceedsMaxAttempts(payload)
	if err != nil {
		queue.DeleteReserved(queueName, bs)
		return err
	}

	if err := this.fire(payload); err != nil {
		return err
	}
	// 执行完删除job
	if err := queue.DeleteReserved(queueName, bs); err != nil {
		return err
	}
	// raiseAfterJobEvent

	return nil
}

func (this *Worker) markJobAsFailedIfAlreadyExceedsMaxAttempts(payload *Payload) error {
	if payload.attempts() > this.option.MaxTries {
		// todo 丢到一个错误队列中去
		// 执行完删除job
		return errors.New("A queued job has been attempted too many times！")
	}
	return nil
}

func (this *Worker) initJob(bs []byte) (*Payload, error) {
	payloadMap := make(map[string]interface{})
	if err := json.Unmarshal(bs, &payloadMap); err != nil {
		return nil, err
	}
	signature, ok := payloadMap["job"].(map[string]interface{})["signature"]
	if !ok {
		return nil, errors.New("couldn't get the signature！")
	}

	job, ok := this.M.JobList.Load(signature.(string))
	if !ok {
		return nil, errors.New("couldn't find the job！")
	}

	// 反射后执行
	jobInstance := reflect.New(reflect.TypeOf(job).Elem()).Interface().(IJob)
	payload := new(Payload)
	payload.Job = jobInstance

	err := json.Unmarshal(bs, &payload)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// Fire the job.
func (this *Worker) fire(payload *Payload) error {
	payload.Job.Execute()

	return nil
}

func (this *Worker) stopIfNecessary() {
	if this.ShouldQuit {
		this.kill(0)
	}
}

func (this *Worker) kill(status int) {
	os.Exit(status)
}

// todo 待修改，改成信道的方式传入
func (this *Worker) listenForSignals() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGCONT)
	go func() {
		for s := range c {
			switch s {
			case syscall.SIGTERM:
				fmt.Println("退出", s)
				this.ShouldQuit = true
			case syscall.SIGUSR2:
				fmt.Println("usr2", s)
				this.Paused = true
			case syscall.SIGCONT:
				this.Paused = false
			default:
				fmt.Println("other", s)
				// nothing to do
			}
		}
	}()
}
