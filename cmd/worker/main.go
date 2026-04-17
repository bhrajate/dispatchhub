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
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence"
	etcdstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/etcd"
	mysqlstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/mysql"
	redisstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/redis"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version"
	workercfg "github.com/dispatchhub/dispatchhub/internal/worker/infrastructure/config"
	workerservice "github.com/dispatchhub/dispatchhub/internal/worker/application/service"
	"github.com/dispatchhub/dispatchhub/internal/worker/interfaces/middleware"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/signals"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	var cfg *workercfg.Config
	var err error
	if configFile != "" {
		cfg, err = workercfg.Load(configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load config: %v\n", err)
			os.Exit(1)
		}
	} else {
		cfg = workercfg.Default()
	}

	log.Init(cfg.Log.Level, cfg.Log.Format, cfg.Log.Output)
	defer log.Sync()

	log.Info(version.Info())
	ctx := signals.SetupSignalContext()

	// --- Infrastructure (factory) ---
	etcdClient, err := persistence.NewEtcdClient(cfg.Etcd)
	if err != nil {
		log.Fatalf("connect etcd: %v", err)
	}
	defer etcdClient.Close()

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

	w.Use(
		middleware.Recovery(),
		middleware.Logging(),
		middleware.Timeout(cfg.Worker.TaskTimeout),
	)

	w.RegisterFunc("example.echo", func(ctx context.Context, task *entity.Task) *entity.TaskResult {
		return &entity.TaskResult{Output: string(task.Payload)}
	})

	w.RegisterFunc("example.sleep", func(ctx context.Context, task *entity.Task) *entity.TaskResult {
		<-ctx.Done()
		return &entity.TaskResult{Error: ctx.Err()}
	})

	// --- Ops server (health + metrics) ---
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

	opsServer := &http.Server{Addr: cfg.Server.HTTPAddr, Handler: opsMux}
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
