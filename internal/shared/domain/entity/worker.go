package entity

import "time"

// WorkerState represents the health state of a worker node.
type WorkerState int

const (
	WorkerStateOnline   WorkerState = iota
	WorkerStateDraining             // accepting no new tasks, finishing current
	WorkerStateOffline
)

var workerStateNames = map[WorkerState]string{
	WorkerStateOnline:   "online",
	WorkerStateDraining: "draining",
	WorkerStateOffline:  "offline",
}

func (s WorkerState) String() string {
	if name, ok := workerStateNames[s]; ok {
		return name
	}
	return "unknown"
}

// WorkerInfo describes a registered worker node in the cluster.
type WorkerInfo struct {
	ID             string            `json:"id"`
	Hostname       string            `json:"hostname"`
	IP             string            `json:"ip"`
	Port           int               `json:"port"`
	State          WorkerState       `json:"state"`
	Labels         map[string]string `json:"labels,omitempty"`
	Queues         []string          `json:"queues"`
	TaskTypes      []string          `json:"task_types,omitempty"`
	Concurrency    int               `json:"concurrency"`
	ActiveTasks    int               `json:"active_tasks"`
	CompletedTotal int64             `json:"completed_total"`
	FailedTotal    int64             `json:"failed_total"`
	CPUUsage       float64           `json:"cpu_usage"`
	MemUsage       float64           `json:"mem_usage"`
	StartedAt      time.Time         `json:"started_at"`
	LastHeartbeat  time.Time         `json:"last_heartbeat"`
	Version        string            `json:"version"`
}

