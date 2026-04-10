package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/scheduler"
	"github.com/dispatchhub/dispatchhub/pkg/types"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Server wraps a gRPC server with DispatchHub API handlers.
type Server struct {
	scheduler *scheduler.Scheduler
	server    *grpc.Server
	health    *health.Server
}

// NewServer creates a new gRPC server with interceptors for
// metrics, recovery, and logging.
func NewServer(sched *scheduler.Scheduler) *Server {
	recoveryOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(func(p interface{}) error {
			log.Errorf("grpc panic recovered: %v", p)
			return status.Errorf(codes.Internal, "internal error")
		}),
	}

	srv := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     5 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 10 * time.Second,
			Time:                  10 * time.Second,
			Timeout:               3 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_prometheus.UnaryServerInterceptor,
			grpc_recovery.UnaryServerInterceptor(recoveryOpts...),
			loggingUnaryInterceptor(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_prometheus.StreamServerInterceptor,
			grpc_recovery.StreamServerInterceptor(recoveryOpts...),
		)),
		grpc.MaxRecvMsgSize(16<<20), // 16MB
	)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv)
	grpc_prometheus.Register(srv)

	return &Server{
		scheduler: sched,
		server:    srv,
		health:    hs,
	}
}

// Serve starts listening on the given address.
func (s *Server) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	log.Infof("gRPC server listening on %s", addr)
	return s.server.Serve(lis)
}

// GracefulStop gracefully shuts down the server.
func (s *Server) GracefulStop() {
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	s.server.GracefulStop()
}

// --- API handlers (implementing the proto service without codegen) ---
// In production, these would be generated from the proto file.
// Here we implement a pattern compatible with manual registration.

// SubmitTask handles task submission.
func (s *Server) SubmitTask(ctx context.Context, name, namespace, group, taskType, queueName string, payload json.RawMessage, priority int, labels map[string]string, maxRetries int, timeout time.Duration) (string, error) {
	task := &types.Task{
		Name:       name,
		Namespace:  namespace,
		Group:      group,
		Type:       taskType,
		Payload:    payload,
		Labels:     labels,
		QueueName:  queueName,
		Priority:   types.TaskPriority(priority),
		MaxRetries: maxRetries,
		Timeout:    types.Duration{Duration: timeout},
	}

	if err := s.scheduler.SubmitTask(ctx, task); err != nil {
		return "", status.Errorf(codes.Internal, "submit task: %v", err)
	}

	return task.ID, nil
}

// GetTask retrieves a task by ID.
func (s *Server) GetTask(ctx context.Context, taskID string) (*types.Task, error) {
	task, err := s.scheduler.GetTask(ctx, taskID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}
	if task == nil {
		return nil, status.Errorf(codes.NotFound, "task %s not found", taskID)
	}
	return task, nil
}

// ListTasks queries tasks.
func (s *Server) ListTasks(ctx context.Context, filter types.TaskFilter) ([]*types.Task, int64, error) {
	tasks, total, err := s.scheduler.ListTasks(ctx, filter)
	if err != nil {
		return nil, 0, status.Errorf(codes.Internal, "list tasks: %v", err)
	}
	return tasks, total, nil
}

// CancelTask cancels a task.
func (s *Server) CancelTask(ctx context.Context, taskID string) error {
	if err := s.scheduler.CancelTask(ctx, taskID); err != nil {
		return status.Errorf(codes.Internal, "cancel task: %v", err)
	}
	return nil
}

// QueueStats returns queue statistics.
func (s *Server) QueueStats(ctx context.Context, queue string) (*types.QueueStats, error) {
	return s.scheduler.QueueStats(ctx, queue)
}

// --- Interceptors ---

func loggingUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		code := codes.OK
		if err != nil {
			if st, ok := status.FromError(err); ok {
				code = st.Code()
			}
		}

		log.Debugf("grpc %s %s %v", info.FullMethod, code, duration)
		return resp, err
	}
}
