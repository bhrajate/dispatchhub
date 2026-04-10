package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Scheduler metrics
	TasksSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "dispatchhub",
		Subsystem: "scheduler",
		Name:      "tasks_submitted_total",
		Help:      "Total number of tasks submitted",
	}, []string{"queue", "type", "priority"})

	TasksScheduled = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "dispatchhub",
		Subsystem: "scheduler",
		Name:      "tasks_scheduled_total",
		Help:      "Total number of tasks dispatched to workers",
	}, []string{"queue", "worker"})

	ScheduleLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "dispatchhub",
		Subsystem: "scheduler",
		Name:      "schedule_latency_seconds",
		Help:      "Time from task submission to scheduling",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"queue"})

	ScheduleLoopDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "dispatchhub",
		Subsystem: "scheduler",
		Name:      "loop_duration_seconds",
		Help:      "Duration of a single scheduling loop iteration",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
	})

	// Worker metrics
	TasksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "dispatchhub",
		Subsystem: "worker",
		Name:      "tasks_processed_total",
		Help:      "Total tasks processed",
	}, []string{"queue", "type", "status"})

	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "dispatchhub",
		Subsystem: "worker",
		Name:      "task_duration_seconds",
		Help:      "Task execution duration",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 15),
	}, []string{"queue", "type"})

	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "dispatchhub",
		Subsystem: "worker",
		Name:      "active_count",
		Help:      "Number of currently active workers",
	})

	ActiveTasks = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "dispatchhub",
		Subsystem: "worker",
		Name:      "active_tasks",
		Help:      "Number of tasks currently being processed",
	}, []string{"queue"})

	// Queue metrics
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "dispatchhub",
		Subsystem: "queue",
		Name:      "depth",
		Help:      "Number of pending tasks in queue",
	}, []string{"queue", "state"})

	// System metrics
	LeaderElections = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "dispatchhub",
		Subsystem: "scheduler",
		Name:      "leader_elections_total",
		Help:      "Total number of leader elections",
	})

	WorkerHeartbeats = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "dispatchhub",
		Subsystem: "worker",
		Name:      "heartbeats_total",
		Help:      "Total heartbeats sent",
	}, []string{"worker"})
)
