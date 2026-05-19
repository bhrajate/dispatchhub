package persistence

import (
	"fmt"

	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/config"
	goredis "github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// NewRedisClient creates a Redis client (standalone or cluster) from config.
func NewRedisClient(cfg config.RedisConfig) goredis.UniversalClient {
	if cfg.ClusterMode {
		return goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:        cfg.ClusterAddrs,
			Password:     cfg.Password,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConns,
			DialTimeout:  cfg.DialTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		})
	}
	return goredis.NewClient(&goredis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
}

// NewMySQLDB creates a GORM DB instance with connection pool tuning from config.
//
// SkipDefaultTransaction is enabled because every Create/Update/Delete in this
// codebase is a single-statement operation that doesn't need GORM's implicit
// BEGIN/COMMIT wrapping. The wrapping costs two extra round-trips per write
// and was the dominant CPU consumer on the API Server write path (~39% of
// SubmitTask time per CPU profile, 2026-05-19).
func NewMySQLDB(cfg config.MySQLConfig) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(cfg.DSN), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, fmt.Errorf("connect mysql: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	if cfg.ConnMaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
	return db, nil
}

// NewEtcdClient creates an etcd client from config.
func NewEtcdClient(cfg config.EtcdConfig) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
		Username:    cfg.Username,
		Password:    cfg.Password,
	})
}
