package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	dispatchpb "github.com/dispatchhub/dispatchhub/api/proto"
	apisvc "github.com/dispatchhub/dispatchhub/internal/apiserver/domain/service"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/pkg/log"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server wraps a gRPC server implementing the DispatchService API.
type Server struct {
	dispatchpb.UnimplementedDispatchServiceServer
	taskSvc apisvc.TaskService
	server  *grpc.Server
	health  *health.Server
}

// NewServer creates a new gRPC server with the DispatchService registered.
func NewServer(taskSvc apisvc.TaskService) *Server {
	recoveryOpts := []grpc_recovery.Option{
		grpc_recovery.WithRecoveryHandler(func(p any) error {
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
		grpc.MaxRecvMsgSize(16<<20),
	)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	reflection.Register(srv)
	grpc_prometheus.Register(srv)

	s := &Server{
		taskSvc: taskSvc,
		server:  srv,
		health:  hs,
	}

	dispatchpb.RegisterDispatchServiceServer(srv, s)

	return s
}

func (s *Server) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	log.Infof("gRPC server listening on %s", addr)
	return s.server.Serve(lis)
}

func (s *Server) GracefulStop() {
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	s.server.GracefulStop()
}

// --- DispatchService implementation ---

func (s *Server) SubmitTask(ctx context.Context, req *dispatchpb.SubmitTaskRequest) (*dispatchpb.SubmitTaskResponse, error) {
	spec := req.GetSpec()
	if spec == nil {
		return nil, status.Errorf(codes.InvalidArgument, "spec is required")
	}
	if spec.GetType() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "type is required")
	}

	task := &entity.Task{
		Name:       spec.GetName(),
		Namespace:  spec.GetNamespace(),
		Group:      spec.GetGroup(),
		Type:       spec.GetType(),
		Payload:    spec.GetPayload(),
		Labels:     spec.GetLabels(),
		Priority:   entity.TaskPriority(spec.GetPriority()),
		QueueName:  spec.GetQueueName(),
		MaxRetries: int(spec.GetMaxRetries()),
	}
	if d := spec.GetDelay(); d != nil {
		task.Delay = entity.Duration{Duration: d.AsDuration()}
	}
	if t := spec.GetScheduleAt(); t != nil {
		st := t.AsTime()
		task.ScheduleAt = &st
	}
	if d := spec.GetTimeout(); d != nil {
		task.Timeout = entity.Duration{Duration: d.AsDuration()}
	}
	if d := spec.GetRetryBackoff(); d != nil {
		task.RetryBackoff = entity.Duration{Duration: d.AsDuration()}
	}

	if err := s.taskSvc.SubmitTask(ctx, task); err != nil {
		return nil, status.Errorf(codes.Internal, "submit task: %v", err)
	}

	return &dispatchpb.SubmitTaskResponse{
		TaskId: task.ID,
		Task:   entityToProtoTask(task),
	}, nil
}

func (s *Server) GetTask(ctx context.Context, req *dispatchpb.GetTaskRequest) (*dispatchpb.GetTaskResponse, error) {
	task, err := s.taskSvc.GetTask(ctx, req.GetTaskId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}
	if task == nil {
		return nil, status.Errorf(codes.NotFound, "task %s not found", req.GetTaskId())
	}
	return &dispatchpb.GetTaskResponse{Task: entityToProtoTask(task)}, nil
}

func (s *Server) ListTasks(ctx context.Context, req *dispatchpb.ListTasksRequest) (*dispatchpb.ListTasksResponse, error) {
	filter := entity.TaskFilter{
		Namespace: req.GetNamespace(),
		Group:     req.GetGroup(),
		Type:      req.GetType(),
		QueueName: req.GetQueueName(),
		Limit:     int(req.GetLimit()),
		Offset:    int(req.GetOffset()),
	}
	if req.GetState() != dispatchpb.TaskState_TASK_STATE_UNSPECIFIED {
		st := protoToEntityState(req.GetState())
		filter.State = &st
	}

	tasks, total, err := s.taskSvc.ListTasks(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list tasks: %v", err)
	}

	pbTasks := make([]*dispatchpb.Task, len(tasks))
	for i, t := range tasks {
		pbTasks[i] = entityToProtoTask(t)
	}
	return &dispatchpb.ListTasksResponse{Tasks: pbTasks, Total: total}, nil
}

func (s *Server) CancelTask(ctx context.Context, req *dispatchpb.CancelTaskRequest) (*dispatchpb.CancelTaskResponse, error) {
	if err := s.taskSvc.CancelTask(ctx, req.GetTaskId()); err != nil {
		return nil, status.Errorf(codes.Internal, "cancel task: %v", err)
	}
	task, err := s.taskSvc.GetTask(ctx, req.GetTaskId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get cancelled task: %v", err)
	}
	return &dispatchpb.CancelTaskResponse{Task: entityToProtoTask(task)}, nil
}

func (s *Server) GetQueueStats(ctx context.Context, req *dispatchpb.GetQueueStatsRequest) (*dispatchpb.GetQueueStatsResponse, error) {
	stats, err := s.taskSvc.QueueStats(ctx, req.GetQueueName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "queue stats: %v", err)
	}
	return &dispatchpb.GetQueueStatsResponse{
		QueueName: stats.Name,
		Pending:   stats.Pending,
		Active:    stats.Active,
		Scheduled: stats.Scheduled,
		Retrying:  stats.Retrying,
		Completed: stats.Completed,
		Failed:    stats.Failed,
	}, nil
}

// --- Conversion helpers ---

func entityToProtoTask(t *entity.Task) *dispatchpb.Task {
	pt := &dispatchpb.Task{
		Id:         t.ID,
		State:      entityToProtoState(t.State),
		Result:     t.Result,
		Error:      t.Error,
		WorkerId:   t.WorkerID,
		RetryCount: int32(t.RetryCount),
		CreatedAt:  timestamppb.New(t.CreatedAt),
		Version:    t.Version,
		Spec: &dispatchpb.TaskSpec{
			Name:       t.Name,
			Namespace:  t.Namespace,
			Group:      t.Group,
			Type:       t.Type,
			Payload:    t.Payload,
			Labels:     t.Labels,
			Priority:   dispatchpb.TaskPriority(t.Priority),
			QueueName:  t.QueueName,
			MaxRetries: int32(t.MaxRetries),
		},
	}
	if t.Delay.Duration > 0 {
		pt.Spec.Delay = durationpb.New(t.Delay.Duration)
	}
	if t.ScheduleAt != nil {
		pt.Spec.ScheduleAt = timestamppb.New(*t.ScheduleAt)
	}
	if t.Timeout.Duration > 0 {
		pt.Spec.Timeout = durationpb.New(t.Timeout.Duration)
	}
	if t.RetryBackoff.Duration > 0 {
		pt.Spec.RetryBackoff = durationpb.New(t.RetryBackoff.Duration)
	}
	if t.StartedAt != nil {
		pt.StartedAt = timestamppb.New(*t.StartedAt)
	}
	if t.FinishedAt != nil {
		pt.FinishedAt = timestamppb.New(*t.FinishedAt)
	}
	return pt
}

func entityToProtoState(s entity.TaskState) dispatchpb.TaskState {
	switch s {
	case entity.TaskStatePending:
		return dispatchpb.TaskState_TASK_STATE_PENDING
	case entity.TaskStateScheduled:
		return dispatchpb.TaskState_TASK_STATE_SCHEDULED
	case entity.TaskStateRunning:
		return dispatchpb.TaskState_TASK_STATE_RUNNING
	case entity.TaskStateRetrying:
		return dispatchpb.TaskState_TASK_STATE_RETRYING
	case entity.TaskStateCompleted:
		return dispatchpb.TaskState_TASK_STATE_COMPLETED
	case entity.TaskStateFailed:
		return dispatchpb.TaskState_TASK_STATE_FAILED
	case entity.TaskStateCancelled:
		return dispatchpb.TaskState_TASK_STATE_CANCELLED
	case entity.TaskStateTimeout:
		return dispatchpb.TaskState_TASK_STATE_TIMEOUT
	default:
		return dispatchpb.TaskState_TASK_STATE_UNSPECIFIED
	}
}

func protoToEntityState(s dispatchpb.TaskState) entity.TaskState {
	switch s {
	case dispatchpb.TaskState_TASK_STATE_PENDING:
		return entity.TaskStatePending
	case dispatchpb.TaskState_TASK_STATE_SCHEDULED:
		return entity.TaskStateScheduled
	case dispatchpb.TaskState_TASK_STATE_RUNNING:
		return entity.TaskStateRunning
	case dispatchpb.TaskState_TASK_STATE_RETRYING:
		return entity.TaskStateRetrying
	case dispatchpb.TaskState_TASK_STATE_COMPLETED:
		return entity.TaskStateCompleted
	case dispatchpb.TaskState_TASK_STATE_FAILED:
		return entity.TaskStateFailed
	case dispatchpb.TaskState_TASK_STATE_CANCELLED:
		return entity.TaskStateCancelled
	case dispatchpb.TaskState_TASK_STATE_TIMEOUT:
		return entity.TaskStateTimeout
	default:
		return entity.TaskStatePending
	}
}

// --- Interceptors ---

func loggingUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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
