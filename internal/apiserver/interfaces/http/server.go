package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	apisvc "github.com/dispatchhub/dispatchhub/internal/apiserver/domain/service"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/pkg/cronutil"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// registerPprofHooks is wired by pprof.go (build tag `pprof`) and by
// pprof_off.go (default). Keeping it as a package var lets us flip pprof
// on with a build tag without leaving handlers exposed in production.
var registerPprofHooks func(*http.ServeMux)

// HealthChecker is called by the readyz endpoint to verify dependencies.
// Returns nil if healthy, or an error describing the unhealthy component.
type HealthChecker func(ctx context.Context) error

// Server provides the REST API and metrics endpoint.
type Server struct {
	taskSvc      apisvc.TaskService
	healthCheck  HealthChecker
	mux          *http.ServeMux
	server       *http.Server
}

// NewServer creates a new HTTP server with REST API routes.
func NewServer(taskSvc apisvc.TaskService, addr string, healthCheck HealthChecker) *Server {
	mux := http.NewServeMux()
	s := &Server{
		taskSvc:     taskSvc,
		healthCheck: healthCheck,
		mux:         mux,
		server: &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}

	mux.HandleFunc("POST /api/v1/tasks", s.handleSubmitTask)
	mux.HandleFunc("GET /api/v1/tasks/{id}", s.handleGetTask)
	mux.HandleFunc("GET /api/v1/tasks", s.handleListTasks)
	mux.HandleFunc("POST /api/v1/tasks/{id}/cancel", s.handleCancelTask)
	mux.HandleFunc("GET /api/v1/queues/{name}/stats", s.handleQueueStats)

	// CronJob routes
	mux.HandleFunc("POST /api/v1/cronjobs", s.handleCreateCronJob)
	mux.HandleFunc("GET /api/v1/cronjobs/{id}", s.handleGetCronJob)
	mux.HandleFunc("GET /api/v1/cronjobs", s.handleListCronJobs)
	mux.HandleFunc("DELETE /api/v1/cronjobs/{id}", s.handleDeleteCronJob)

	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	registerPprofHooks(mux)

	return s
}

func (s *Server) Serve() error {
	log.Infof("HTTP server listening on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string            `json:"name"`
		Namespace  string            `json:"namespace"`
		Group      string            `json:"group"`
		Type       string            `json:"type"`
		Payload    json.RawMessage   `json:"payload"`
		Labels     map[string]string `json:"labels"`
		Priority   int               `json:"priority"`
		QueueName  string            `json:"queue_name"`
		MaxRetries int               `json:"max_retries"`
		Timeout    string            `json:"timeout"`
		Delay      string            `json:"delay"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: %v", err)
		return
	}

	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}

	task := &entity.Task{
		Name:       req.Name,
		Namespace:  req.Namespace,
		Group:      req.Group,
		Type:       req.Type,
		Payload:    req.Payload,
		Labels:     req.Labels,
		Priority:   entity.TaskPriority(req.Priority),
		QueueName:  req.QueueName,
		MaxRetries: req.MaxRetries,
	}

	if req.Timeout != "" {
		if d, err := time.ParseDuration(req.Timeout); err == nil {
			task.Timeout = entity.Duration{Duration: d}
		}
	}
	if req.Delay != "" {
		if d, err := time.ParseDuration(req.Delay); err == nil {
			task.Delay = entity.Duration{Duration: d}
		}
	}

	if err := s.taskSvc.SubmitTask(r.Context(), task); err != nil {
		log.Errorf("submit task: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to submit task")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"task_id": task.ID,
		"task":    task,
	})
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	task, err := s.taskSvc.GetTask(r.Context(), id)
	if err != nil {
		log.Errorf("get task: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to get task")
		return
	}
	if task == nil {
		writeError(w, http.StatusNotFound, "task %s not found", id)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := entity.TaskFilter{
		Namespace: q.Get("namespace"),
		Group:     q.Get("group"),
		Type:      q.Get("type"),
		QueueName: q.Get("queue"),
	}

	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Offset = n
		}
	}

	tasks, total, err := s.taskSvc.ListTasks(r.Context(), filter)
	if err != nil {
		log.Errorf("list tasks: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to list tasks")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"tasks": tasks,
		"total": total,
	})
}

func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.taskSvc.CancelTask(r.Context(), id); err != nil {
		log.Errorf("cancel task: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel task")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (s *Server) handleQueueStats(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	stats, err := s.taskSvc.QueueStats(r.Context(), name)
	if err != nil {
		log.Errorf("queue stats: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to get queue stats")
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// --- CronJob handlers ---

func (s *Server) handleCreateCronJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name         string            `json:"name"`
		Namespace    string            `json:"namespace"`
		Type         string            `json:"type"`
		Payload      json.RawMessage   `json:"payload"`
		Labels       map[string]string `json:"labels"`
		CronExpr     string            `json:"cron_expr"`
		QueueName    string            `json:"queue_name"`
		Priority     int               `json:"priority"`
		MaxRetries   int               `json:"max_retries"`
		Timeout      string            `json:"timeout"`
		RetryBackoff string            `json:"retry_backoff"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: %v", err)
		return
	}
	if req.Type == "" || req.CronExpr == "" {
		writeError(w, http.StatusBadRequest, "type and cron_expr are required")
		return
	}

	job := &entity.CronJob{
		Name:       req.Name,
		Namespace:  req.Namespace,
		Type:       req.Type,
		Payload:    req.Payload,
		Labels:     req.Labels,
		CronExpr:   req.CronExpr,
		QueueName:  req.QueueName,
		Priority:   entity.TaskPriority(req.Priority),
		MaxRetries: req.MaxRetries,
		Enabled:    true,
	}
	if req.Timeout != "" {
		if d, err := time.ParseDuration(req.Timeout); err == nil {
			job.Timeout = entity.Duration{Duration: d}
		}
	}
	if req.RetryBackoff != "" {
		if d, err := time.ParseDuration(req.RetryBackoff); err == nil {
			job.RetryBackoff = entity.Duration{Duration: d}
		}
	}

	// Parse cron expression and compute initial next_run_at (infrastructure concern, not domain)
	next, err := cronutil.NextRunTime(req.CronExpr, time.Now())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid cron expression: %v", err)
		return
	}
	job.NextRunAt = &next

	if err := s.taskSvc.CreateCronJob(r.Context(), job); err != nil {
		log.Errorf("create cron job: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to create cron job")
		return
	}

	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleGetCronJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.taskSvc.GetCronJob(r.Context(), id)
	if err != nil {
		log.Errorf("get cron job: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to get cron job")
		return
	}
	if job == nil {
		writeError(w, http.StatusNotFound, "cron job %s not found", id)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleListCronJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	namespace := q.Get("namespace")
	limit, offset := 100, 0
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			offset = n
		}
	}

	jobs, total, err := s.taskSvc.ListCronJobs(r.Context(), namespace, limit, offset)
	if err != nil {
		log.Errorf("list cron jobs: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to list cron jobs")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"cron_jobs": jobs,
		"total":     total,
	})
}

func (s *Server) handleDeleteCronJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.taskSvc.DeleteCronJob(r.Context(), id); err != nil {
		log.Errorf("delete cron job: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to delete cron job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.healthCheck != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := s.healthCheck(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	writeJSON(w, code, map[string]string{"error": msg})
}
