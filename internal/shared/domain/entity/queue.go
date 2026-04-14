package entity

// QueueStats holds real-time statistics for a queue.
type QueueStats struct {
	Name      string `json:"name"`
	Pending   int64  `json:"pending"`
	Active    int64  `json:"active"`
	Scheduled int64  `json:"scheduled"`
	Retrying  int64  `json:"retrying"`
	Completed int64  `json:"completed"`
	Failed    int64  `json:"failed"`
}

// DefaultQueueName is the queue used when none is specified.
const DefaultQueueName = "default"
