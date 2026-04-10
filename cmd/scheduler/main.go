package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/version"
	"github.com/dispatchhub/dispatchhub/pkg/config"
	"github.com/dispatchhub/dispatchhub/pkg/election"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/metrics"
	"github.com/dispatchhub/dispatchhub/pkg/scheduler"
	"github.com/dispatchhub/dispatchhub/pkg/signals"
	redisstore "github.com/dispatchhub/dispatchhub/pkg/store/redis"
	etcdstore "github.com/dispatchhub/dispatchhub/pkg/store/etcd"
	mysqlstore "github.com/dispatchhub/dispatchhub/pkg/store/mysql"

	grpcapi "github.com/dispatchhub/dispatchhub/pkg/api/grpc"
	httpapi "github.com/dispatchhub/dispatchhub/pkg/api/http"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	_ "github.com/dispatchhub/dispatchhub/pkg/metrics" // register metrics
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

	// Load config
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

	// Initialize logger
	log.Init(cfg.Log.Level, cfg.Log.Format, cfg.Log.Output)
	defer log.Sync()

	log.Info(version.Info())

	// Setup signal handler
	ctx := signals.SetupSignalContext()

	// --- Initialize infrastructure ---

	// etcd client
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: cfg.Etcd.DialTimeout,
		Username:    cfg.Etcd.Username,
		Password:    cfg.Etcd.Password,
	})
	if err != nil {
		log.Fatalf("connect etcd: %v", err)
	}
	defer etcdClient.Close()

	// Redis client
	var redisClient goredis.UniversalClient
	if cfg.Redis.ClusterMode {
		redisClient = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:        cfg.Redis.ClusterAddrs,
			Password:     cfg.Redis.Password,
			PoolSize:     cfg.Redis.PoolSize,
			MinIdleConns: cfg.Redis.MinIdleConns,
			DialTimeout:  cfg.Redis.DialTimeout,
			ReadTimeout:  cfg.Redis.ReadTimeout,
			WriteTimeout: cfg.Redis.WriteTimeout,
		})
	} else {
		redisClient = goredis.NewClient(&goredis.Options{
			Addr:         cfg.Redis.Addr,
			Password:     cfg.Redis.Password,
			DB:           cfg.Redis.DB,
			PoolSize:     cfg.Redis.PoolSize,
			MinIdleConns: cfg.Redis.MinIdleConns,
			DialTimeout:  cfg.Redis.DialTimeout,
			ReadTimeout:  cfg.Redis.ReadTimeout,
			WriteTimeout: cfg.Redis.WriteTimeout,
		})
	}
	defer redisClient.Close()

	// MySQL
	db, err := gorm.Open(mysql.Open(cfg.MySQL.DSN), &gorm.Config{})
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(cfg.MySQL.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MySQL.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.MySQL.ConnMaxLifetime)
	defer sqlDB.Close()

	// --- Build stores ---
	broker := redisstore.NewQueueBroker(redisClient)
	registry := etcdstore.NewRegistry(etcdClient)
	taskStore, err := mysqlstore.NewTaskStore(db)
	if err != nil {
		log.Fatalf("init task store: %v", err)
	}

	// --- Build scheduler ---
	sched := scheduler.New(cfg.Scheduler, broker, taskStore, registry)

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
			if err := sched.Run(leaderCtx); err != nil {
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

	// Start leader election in background
	go func() {
		if err := le.Run(ctx); err != nil {
			log.Errorf("leader election: %v", err)
		}
	}()

	// --- Start API servers ---
	// gRPC always runs (forwards to leader if not leader)
	grpcServer := grpcapi.NewServer(sched)
	go func() {
		if err := grpcServer.Serve(cfg.Server.GRPCAddr); err != nil {
			log.Fatalf("gRPC server: %v", err)
		}
	}()

	// HTTP REST + metrics
	httpServer := httpapi.NewServer(sched, cfg.Server.HTTPAddr)
	go func() {
		if err := httpServer.Serve(); err != nil {
			log.Errorf("HTTP server: %v", err)
		}
	}()

	// Wait for shutdown
	<-ctx.Done()

	log.Info("shutting down scheduler...")
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(ctx)
	log.Info("scheduler shutdown complete")
}
