package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/config"
	etcdstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/etcd"
	mysqlstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/mysql"
	redisstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/redis"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version"
	workerservice "github.com/dispatchhub/dispatchhub/internal/worker/application/service"
	"github.com/dispatchhub/dispatchhub/internal/worker/interfaces/middleware"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/signals"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	goredis "github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
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

	// --- Infrastructure ---
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: cfg.Etcd.DialTimeout,
	})
	if err != nil {
		log.Fatalf("connect etcd: %v", err)
	}
	defer etcdClient.Close()

	var redisClient goredis.UniversalClient
	if cfg.Redis.ClusterMode {
		redisClient = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:        cfg.Redis.ClusterAddrs,
			Password:     cfg.Redis.Password,
			PoolSize:     cfg.Redis.PoolSize,
			MinIdleConns: cfg.Redis.MinIdleConns,
		})
	} else {
		redisClient = goredis.NewClient(&goredis.Options{
			Addr:         cfg.Redis.Addr,
			Password:     cfg.Redis.Password,
			DB:           cfg.Redis.DB,
			PoolSize:     cfg.Redis.PoolSize,
			MinIdleConns: cfg.Redis.MinIdleConns,
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
	registry := etcdstore.NewWorkerRegistry(etcdClient)
	taskRepo, err := mysqlstore.NewTaskRepository(db)
	if err != nil {
		log.Fatalf("init task repository: %v", err)
	}

	// --- Worker application service ---
	w := workerservice.NewWorkerAppService(
		workerservice.WorkerConfig{
			ID:                cfg.Worker.ID,
			Queues:            cfg.Worker.Queues,
			Concurrency:       cfg.Worker.Concurrency,
			HeartbeatInterval: cfg.Worker.HeartbeatInterval,
			ShutdownTimeout:   cfg.Worker.ShutdownTimeout,
			TaskTimeout:       cfg.Worker.TaskTimeout,
		},
		broker, registry, taskRepo,
	)

	// --- Worker middleware (interfaces layer) ---
	w.Use(
		middleware.Recovery(),
		middleware.Logging(),
		middleware.Timeout(cfg.Worker.TaskTimeout),
	)

	// Register example handlers
	w.RegisterFunc("example.echo", func(ctx context.Context, task *entity.Task) *entity.TaskResult {
		return &entity.TaskResult{Output: string(task.Payload)}
	})

	w.RegisterFunc("example.sleep", func(ctx context.Context, task *entity.Task) *entity.TaskResult {
		<-ctx.Done()
		return &entity.TaskResult{Error: ctx.Err()}
	})

	// --- Internal ops server (health + metrics only) ---
	opsMux := http.NewServeMux()
	opsMux.Handle("GET /metrics", promhttp.Handler())
	opsMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	opsMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})

	opsServer := &http.Server{
		Addr:    cfg.Server.HTTPAddr,
		Handler: opsMux,
	}
	go func() {
		log.Infof("worker ops server listening on %s", cfg.Server.HTTPAddr)
		if err := opsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("ops server: %v", err)
		}
	}()

	if err := w.Run(ctx); err != nil {
		log.Fatalf("worker: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = opsServer.Shutdown(shutdownCtx)
	log.Info("worker shutdown complete")
}
