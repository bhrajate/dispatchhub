package persistence

import (
	"fmt"

	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/config"
	goredis "github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// NewRedisClient 根据配置创建 Redis client（standalone 或 cluster）。
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

// NewMySQLDB 根据配置创建带连接池调优的 GORM DB 实例。
//
// 启用 SkipDefaultTransaction 是因为本代码库中的每个 Create/Update/Delete
// 都是单语句操作，不需要 GORM 隐式的 BEGIN/COMMIT 包装。
// 该包装会让每次写入增加两次额外 round-trip，
// 并且是 API Server 写入路径上的主要 CPU 消耗来源
// （2026-05-19 CPU profile 中约占 SubmitTask 时间的 39%）。
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

// NewEtcdClient 根据配置创建 etcd client。
func NewEtcdClient(cfg config.EtcdConfig) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
		Username:    cfg.Username,
		Password:    cfg.Password,
	})
}
