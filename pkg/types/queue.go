package types

// QueueConfig defines the configuration for a named queue.
type QueueConfig struct {
	Name        string `json:"name"`
	Priority    int    `json:"priority"`     // higher = dequeued first when multiple queues compete
	MaxSize     int64  `json:"max_size"`     // 0 = unlimited
	RateLimit   int    `json:"rate_limit"`   // tasks per second, 0 = unlimited
	Concurrency int    `json:"concurrency"`  // max concurrent tasks from this queue per worker
	Paused      bool   `json:"paused"`
}

// QueueStats holds real-time statistics for a queue.
type QueueStats struct {
	Name      string `json:"name"`
	Pending   int64  `json:"pending"`
	Active    int64  `json:"active"`
	Scheduled int64  `json:"scheduled"` // delayed tasks
	Retrying  int64  `json:"retrying"`
	Completed int64  `json:"completed"`
	Failed    int64  `json:"failed"`
}

// DefaultQueueName is the queue used when none is specified.
const DefaultQueueName = "default"
