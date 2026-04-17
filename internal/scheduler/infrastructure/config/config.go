package config

import (
	"time"

	sharedcfg "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/config"
)

type SchedulerConfig struct {
	LeaseDuration     time.Duration `yaml:"lease_duration"`
	CronCheckInterval time.Duration `yaml:"cron_check_interval"`
	CronBatchSize     int           `yaml:"cron_batch_size"`
}

type Config struct {
	Server    sharedcfg.ServerConfig `yaml:"server"`
	Scheduler SchedulerConfig        `yaml:"scheduler"`
	Etcd      sharedcfg.EtcdConfig   `yaml:"etcd"`
	Redis     sharedcfg.RedisConfig  `yaml:"redis"`
	MySQL     sharedcfg.MySQLConfig  `yaml:"mysql"`
	Metrics   sharedcfg.MetricsConfig `yaml:"metrics"`
	Log       sharedcfg.LogConfig    `yaml:"log"`
}

func Default() *Config {
	return &Config{
		Server: sharedcfg.ServerConfig{
			HTTPAddr: ":8080",
		},
		Scheduler: SchedulerConfig{
			LeaseDuration:     15 * time.Second,
			CronCheckInterval: time.Second,
			CronBatchSize:     100,
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
