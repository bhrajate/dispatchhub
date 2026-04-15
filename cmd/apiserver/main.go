package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	apisvc "github.com/dispatchhub/dispatchhub/internal/apiserver/domain/service"
	apiservergrpc "github.com/dispatchhub/dispatchhub/internal/apiserver/interfaces/grpc"
	apiserverhttp "github.com/dispatchhub/dispatchhub/internal/apiserver/interfaces/http"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/config"
	mysqlstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/mysql"
	redisstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/redis"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/metrics"
	"github.com/dispatchhub/dispatchhub/pkg/ratelimit"
	"github.com/dispatchhub/dispatchhub/pkg/signals"
)

func main() {
	var (
		configFile  string
		showVersion bool
	)
	flag.StringVar(&configFile, "config", "", "path to config file")
	flag.BoolVar(&showVersion, "version", false, "show version")
	flag.Parse()

	if showVersion {
		fmt.Println(version.Info())
		os.Exit(0)
	}

	var cfg *config.Config
	var err error
	if configFile != "" {
		cfg, err = config.LoadFromFile(configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load config: %v\n", err)
			os.Exit(1)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	log.Init(cfg.Log.Level, cfg.Log.Format, cfg.Log.Output)
	defer log.Sync()

	log.Info(version.Info())
	ctx := signals.SetupSignalContext()

	// --- Infrastructure (factory) ---
	redisClient := persistence.NewRedisClient(cfg.Redis)
	defer redisClient.Close()

	db, err := persistence.NewMySQLDB(cfg.MySQL)
	if err != nil {
		log.Fatalf("%v", err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	// --- Repositories ---
	broker := redisstore.NewQueueBroker(redisClient)
	taskRepo, err := mysqlstore.NewTaskRepository(db)
	if err != nil {
		log.Fatalf("init task repository: %v", err)
	}
	cronRepo, err := mysqlstore.NewCronJobRepository(db)
	if err != nil {
		log.Fatalf("init cron job repository: %v", err)
	}

	// --- TaskService ---
	taskSvc := apisvc.NewTaskServiceImpl(broker, taskRepo, cronRepo)

	limiter := ratelimit.NewMultiQueueLimiter(1000, 1000)
	taskSvc.SetBeforeSubmit(func(task *entity.Task) error {
		if !limiter.Allow(task.QueueName) {
			return fmt.Errorf("queue %s rate limit exceeded", task.QueueName)
		}
		return nil
	})

	taskSvc.SetAfterSubmit(func(task *entity.Task) {
		metrics.TasksSubmitted.WithLabelValues(
			task.QueueName, task.Type, fmt.Sprintf("%d", task.Priority),
		).Inc()
		log.Infof("task submitted: id=%s type=%s queue=%s priority=%d",
			task.ID, task.Type, task.QueueName, task.Priority)
	})

	// --- Interfaces ---
	grpcServer := apiservergrpc.NewServer(taskSvc)
	go func() {
		if err := grpcServer.Serve(cfg.Server.GRPCAddr); err != nil {
			log.Fatalf("gRPC server: %v", err)
		}
	}()

	httpServer := apiserverhttp.NewServer(taskSvc, cfg.Server.HTTPAddr, func(ctx context.Context) error {
		if err := redisClient.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		checkDB, _ := db.DB()
		if err := checkDB.PingContext(ctx); err != nil {
			return fmt.Errorf("mysql: %w", err)
		}
		return nil
	})
	go func() {
		if err := httpServer.Serve(); err != nil {
			log.Errorf("HTTP server: %v", err)
		}
	}()

	<-ctx.Done()

	log.Info("shutting down apiserver...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(shutdownCtx)
	log.Info("apiserver shutdown complete")
}
