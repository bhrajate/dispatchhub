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
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/config"
	mysqlstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/mysql"
	redisstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/redis"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/metrics"
	"github.com/dispatchhub/dispatchhub/pkg/ratelimit"
	"github.com/dispatchhub/dispatchhub/pkg/signals"

	goredis "github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// apiserver is a stateless API gateway that can be horizontally scaled.
// It does not participate in leader election — it simply exposes the
// HTTP/gRPC API and delegates to the shared TaskService + repositories.
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

	// --- Infrastructure ---
	var redisClient goredis.UniversalClient
	if cfg.Redis.ClusterMode {
		redisClient = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:    cfg.Redis.ClusterAddrs,
			Password: cfg.Redis.Password,
			PoolSize: cfg.Redis.PoolSize,
		})
	} else {
		redisClient = goredis.NewClient(&goredis.Options{
			Addr:     cfg.Redis.Addr,
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.DB,
			PoolSize: cfg.Redis.PoolSize,
		})
	}
	defer redisClient.Close()

	db, err := gorm.Open(mysql.Open(cfg.MySQL.DSN), &gorm.Config{})
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("get sql.DB: %v", err)
	}
	sqlDB.SetMaxOpenConns(cfg.MySQL.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MySQL.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.MySQL.ConnMaxLifetime)
	defer sqlDB.Close()

	// --- Repositories (shared infrastructure) ---
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

	// Rate limiting (per-queue token bucket)
	limiter := ratelimit.NewMultiQueueLimiter(1000, 1000) // default: 1000 req/s per queue
	taskSvc.SetBeforeSubmit(func(task *entity.Task) error {
		if !limiter.Allow(task.QueueName) {
			return fmt.Errorf("queue %s rate limit exceeded", task.QueueName)
		}
		return nil
	})

	// Logging + metrics
	taskSvc.SetAfterSubmit(func(task *entity.Task) {
		metrics.TasksSubmitted.WithLabelValues(
			task.QueueName, task.Type, fmt.Sprintf("%d", task.Priority),
		).Inc()
		log.Infof("task submitted: id=%s type=%s queue=%s priority=%d",
			task.ID, task.Type, task.QueueName, task.Priority)
	})

	// --- Apiserver interfaces ---
	grpcServer := apiservergrpc.NewServer(taskSvc)
	go func() {
		if err := grpcServer.Serve(cfg.Server.GRPCAddr); err != nil {
			log.Fatalf("gRPC server: %v", err)
		}
	}()

	httpServer := apiserverhttp.NewServer(taskSvc, cfg.Server.HTTPAddr)
	go func() {
		if err := httpServer.Serve(); err != nil {
			log.Errorf("HTTP server: %v", err)
		}
	}()

	<-ctx.Done()

	log.Info("shutting down apiserver...")
	// Use fresh context for graceful shutdown (original ctx is already cancelled)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(shutdownCtx)
	log.Info("apiserver shutdown complete")
}
