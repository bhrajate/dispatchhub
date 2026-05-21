package entity

// QueueStats 保存队列的实时统计信息。
type QueueStats struct {
	Name      string `json:"name"`
	Pending   int64  `json:"pending"`
	Active    int64  `json:"active"`
	Scheduled int64  `json:"scheduled"`
	Retrying  int64  `json:"retrying"`
	Completed int64  `json:"completed"`
	Failed    int64  `json:"failed"`
}

// DefaultQueueName 是未指定队列时使用的默认队列。
const DefaultQueueName = "default"
