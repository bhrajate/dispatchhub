package config

import (
	"time"

	sharedcfg "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/config"
)

type WorkerConfig struct {
	ID                string        `yaml:"id"`
	Queues            []string      `yaml:"queues"`
	Concurrency       int           `yaml:"concurrency"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
	TaskTimeout       time.Duration `yaml:"task_timeout"`
}

type Config struct {
	Server  sharedcfg.ServerConfig  `yaml:"server"`
	Worker  WorkerConfig            `yaml:"worker"`
	Etcd    sharedcfg.EtcdConfig    `yaml:"etcd"`
	Redis   sharedcfg.RedisConfig   `yaml:"redis"`
	MySQL   sharedcfg.MySQLConfig   `yaml:"mysql"`
	Metrics sharedcfg.MetricsConfig `yaml:"metrics"`
	Log     sharedcfg.LogConfig     `yaml:"log"`
}

func Default() *Config {
	return &Config{
		Server: sharedcfg.ServerConfig{
			HTTPAddr: ":8080",
		},
		Worker: WorkerConfig{
			Queues:            []string{"default"},
			Concurrency:       100,
			HeartbeatInterval: 5 * time.Second,
			ShutdownTimeout:   30 * time.Second,
			TaskTimeout:       5 * time.Minute,
		},
		Etcd:    sharedcfg.DefaultEtcdConfig(),
		Redis:   sharedcfg.DefaultRedisConfig(),
		MySQL:   sharedcfg.DefaultMySQLConfig(),
		Metrics: sharedcfg.DefaultMetricsConfig(),
		Log:     sharedcfg.DefaultLogConfig(),
	}
}

func Load(path string) (*Config, error) {
	cfg := Default()
	if err := sharedcfg.LoadYAML(path, cfg); err != nil {
		return nil, err
	}
	sharedcfg.ApplyServerEnvOverrides(&cfg.Server)
	sharedcfg.ApplyEtcdEnvOverrides(&cfg.Etcd)
	sharedcfg.ApplyRedisEnvOverrides(&cfg.Redis)
	sharedcfg.ApplyMySQLEnvOverrides(&cfg.MySQL)
	sharedcfg.ApplyLogEnvOverrides(&cfg.Log)
	return cfg, nil
}
