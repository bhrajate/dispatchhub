package config

import (
	sharedcfg "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/config"
)

type Config struct {
	Server  sharedcfg.ServerConfig  `yaml:"server"`
	Etcd    sharedcfg.EtcdConfig    `yaml:"etcd"`
	Redis   sharedcfg.RedisConfig   `yaml:"redis"`
	MySQL   sharedcfg.MySQLConfig   `yaml:"mysql"`
	Metrics sharedcfg.MetricsConfig `yaml:"metrics"`
	Log     sharedcfg.LogConfig     `yaml:"log"`
}

func Default() *Config {
	return &Config{
		Server: sharedcfg.ServerConfig{
			GRPCAddr: ":9090",
			HTTPAddr: ":8080",
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
