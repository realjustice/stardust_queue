package queue

// job option
type JobOption struct {
	QueueName string `json:"queue"`
	Delay     int    `json:"delay"`
	Connector string `json:"connector"`
}
