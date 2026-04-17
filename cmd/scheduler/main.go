package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/scheduler/application"
	schedulercfg "github.com/dispatchhub/dispatchhub/internal/scheduler/infrastructure/config"
	scheddomainsvc "github.com/dispatchhub/dispatchhub/internal/scheduler/domain/service"
	"github.com/dispatchhub/dispatchhub/internal/scheduler/infrastructure/election"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence"
	etcdstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/etcd"
	mysqlstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/mysql"
	redisstore "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/persistence/redis"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/metrics"
	"github.com/dispatchhub/dispatchhub/pkg/signals"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/google/uuid"

	_ "github.com/dispatchhub/dispatchhub/pkg/metrics"
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

	var cfg *schedulercfg.Config
	var err error
	if configFile != "" {
		cfg, err = schedulercfg.Load(configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load config: %v\n", err)
			os.Exit(1)
		}
	} else {
		cfg = schedulercfg.Default()
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
	cronRepo, err := mysqlstore.NewCronJobRepository(db)
	if err != nil {
		log.Fatalf("init cron job repository: %v", err)
	}

	// --- Scheduler domain + application service ---
	domainSvc := scheddomainsvc.NewSchedulerService(broker, taskRepo, cronRepo, registry)

	appCfg := application.DefaultSchedulerAppConfig()
	if cfg.Scheduler.CronCheckInterval > 0 {
		appCfg.CronCheckInterval = cfg.Scheduler.CronCheckInterval
	}
	if cfg.Scheduler.CronBatchSize > 0 {
		appCfg.CronBatchSize = cfg.Scheduler.CronBatchSize
	}
	schedulerApp := application.NewSchedulerAppService(appCfg, domainSvc)

	// --- Leader election ---
	schedulerID := fmt.Sprintf("scheduler-%s", uuid.New().String()[:8])
	le := election.New(election.Config{
		Client:         etcdClient,
		ElectionPrefix: "/dispatchhub/scheduler/leader",
		ID:             schedulerID,
		TTL:            int(cfg.Scheduler.LeaseDuration / time.Second),
		OnStartedLeading: func(leaderCtx context.Context) {
			log.Infof("this instance is now the leader: %s", schedulerID)
			metrics.LeaderElections.Inc()
			if err := schedulerApp.Run(leaderCtx); err != nil {
				log.Errorf("scheduler run error: %v", err)
			}
		},
		OnStoppedLeading: func() {
			log.Warn("lost leadership, stopping scheduler loops")
		},
		OnNewLeader: func(identity string) {
			log.Infof("new leader elected: %s", identity)
		},
	})

	go func() {
		if err := le.Run(ctx); err != nil {
			log.Errorf("leader election: %v", err)
		}
	}()

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
		log.Infof("scheduler ops server listening on %s", cfg.Server.HTTPAddr)
		if err := opsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("ops server: %v", err)
		}
	}()

	<-ctx.Done()

	log.Info("shutting down scheduler...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = opsServer.Shutdown(shutdownCtx)
	log.Info("scheduler shutdown complete")
}
