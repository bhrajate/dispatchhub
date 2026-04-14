package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	apisvc "github.com/dispatchhub/dispatchhub/internal/apiserver/domain/service"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server provides the REST API and metrics endpoint.
type Server struct {
	taskSvc apisvc.TaskService
	mux     *http.ServeMux
	server  *http.Server
}

// NewServer creates a new HTTP server with REST API routes.
func NewServer(taskSvc apisvc.TaskService, addr string) *Server {
	mux := http.NewServeMux()
	s := &Server{
		taskSvc: taskSvc,
		mux:     mux,
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

	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

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
		writeError(w, http.StatusInternalServerError, "submit task: %v", err)
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
		writeError(w, http.StatusInternalServerError, "get task: %v", err)
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
		writeError(w, http.StatusInternalServerError, "list tasks: %v", err)
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
		writeError(w, http.StatusInternalServerError, "cancel task: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (s *Server) handleQueueStats(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	stats, err := s.taskSvc.QueueStats(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "queue stats: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
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
