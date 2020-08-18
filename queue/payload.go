package queue

type Payload struct {
	ID       JobID `json:"id"`        // job ID
	MaxTries int   `json:"max_tries"` // 最大重试次数
	Attempts int   `json:"attempts"`  // 已重试次数
	Job      IJob  `json:"job"`       // job实体
}

func (this *Payload) attempts() int {
	return this.Attempts
}
