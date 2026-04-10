package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dispatchhub/dispatchhub/internal/version"
	"github.com/dispatchhub/dispatchhub/pkg/config"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/scheduler"
	"github.com/dispatchhub/dispatchhub/pkg/signals"
	etcdstore "github.com/dispatchhub/dispatchhub/pkg/store/etcd"
	mysqlstore "github.com/dispatchhub/dispatchhub/pkg/store/mysql"
	redisstore "github.com/dispatchhub/dispatchhub/pkg/store/redis"

	grpcapi "github.com/dispatchhub/dispatchhub/pkg/api/grpc"
	httpapi "github.com/dispatchhub/dispatchhub/pkg/api/http"

	goredis "github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// apiserver is a stateless API gateway that can be horizontally scaled.
// It does not participate in leader election — it simply exposes the
// HTTP/gRPC API and delegates to the scheduler + stores.
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

	// Infrastructure
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
	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	broker := redisstore.NewQueueBroker(redisClient)
	registry := etcdstore.NewRegistry(etcdClient)
	taskStore, err := mysqlstore.NewTaskStore(db)
	if err != nil {
		log.Fatalf("init task store: %v", err)
	}

	sched := scheduler.New(cfg.Scheduler, broker, taskStore, registry)

	// Start servers
	grpcServer := grpcapi.NewServer(sched)
	go func() {
		if err := grpcServer.Serve(cfg.Server.GRPCAddr); err != nil {
			log.Fatalf("gRPC server: %v", err)
		}
	}()

	httpServer := httpapi.NewServer(sched, cfg.Server.HTTPAddr)
	go func() {
		if err := httpServer.Serve(); err != nil {
			log.Errorf("HTTP server: %v", err)
		}
	}()

	<-ctx.Done()

	log.Info("shutting down apiserver...")
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(ctx)
	log.Info("apiserver shutdown complete")
}
