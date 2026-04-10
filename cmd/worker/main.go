package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/dispatchhub/dispatchhub/internal/version"
	"github.com/dispatchhub/dispatchhub/pkg/config"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/middleware"
	"github.com/dispatchhub/dispatchhub/pkg/signals"
	"github.com/dispatchhub/dispatchhub/pkg/types"
	"github.com/dispatchhub/dispatchhub/pkg/worker"
	etcdstore "github.com/dispatchhub/dispatchhub/pkg/store/etcd"
	mysqlstore "github.com/dispatchhub/dispatchhub/pkg/store/mysql"
	redisstore "github.com/dispatchhub/dispatchhub/pkg/store/redis"

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

	// etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: cfg.Etcd.DialTimeout,
	})
	if err != nil {
		log.Fatalf("connect etcd: %v", err)
	}
	defer etcdClient.Close()

	// Redis
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

	// Build stores
	broker := redisstore.NewQueueBroker(redisClient)
	registry := etcdstore.NewRegistry(etcdClient)
	taskStore, err := mysqlstore.NewTaskStore(db)
	if err != nil {
		log.Fatalf("init task store: %v", err)
	}

	// Build worker
	w := worker.New(cfg.Worker, broker, registry, taskStore)

	// Apply middleware
	w.Use(
		middleware.Recovery(),
		middleware.Logging(),
		middleware.Timeout(cfg.Worker.TaskTimeout),
	)

	// Register example handlers (in production, these would be loaded dynamically or from plugins)
	w.RegisterFunc("example.echo", func(ctx context.Context, task *types.Task) *types.TaskResult {
		return &types.TaskResult{Output: string(task.Payload)}
	})

	w.RegisterFunc("example.sleep", func(ctx context.Context, task *types.Task) *types.TaskResult {
		select {
		case <-ctx.Done():
			return &types.TaskResult{Error: ctx.Err()}
		}
	})

	// Run the worker (blocks until shutdown)
	if err := w.Run(ctx); err != nil {
		log.Fatalf("worker: %v", err)
	}

	log.Info("worker shutdown complete")
}
