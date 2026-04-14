package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration for all DispatchHub components.
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Worker    WorkerConfig    `yaml:"worker"`
	Etcd      EtcdConfig      `yaml:"etcd"`
	Redis     RedisConfig     `yaml:"redis"`
	MySQL     MySQLConfig     `yaml:"mysql"`
	Metrics   MetricsConfig   `yaml:"metrics"`
	Log       LogConfig       `yaml:"log"`
}

type ServerConfig struct {
	GRPCAddr string `yaml:"grpc_addr" env:"DISPATCH_GRPC_ADDR"`
	HTTPAddr string `yaml:"http_addr" env:"DISPATCH_HTTP_ADDR"`
}

type SchedulerConfig struct {
	LeaseDuration     time.Duration `yaml:"lease_duration"`
	CronCheckInterval time.Duration `yaml:"cron_check_interval"`
	CronBatchSize     int           `yaml:"cron_batch_size"`
}

type WorkerConfig struct {
	ID                string        `yaml:"id"`
	Queues            []string      `yaml:"queues"`
	Concurrency       int           `yaml:"concurrency"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
	TaskTimeout       time.Duration `yaml:"task_timeout"`
}

type EtcdConfig struct {
	Endpoints   []string      `yaml:"endpoints"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
	Username    string        `yaml:"username"`
	Password    string        `yaml:"password"`
	TLSCert     string        `yaml:"tls_cert"`
	TLSKey      string        `yaml:"tls_key"`
	TLSCA       string        `yaml:"tls_ca"`
}

type RedisConfig struct {
	Addr         string        `yaml:"addr"`
	Password     string        `yaml:"password"`
	DB           int           `yaml:"db"`
	PoolSize     int           `yaml:"pool_size"`
	MinIdleConns int           `yaml:"min_idle_conns"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	ClusterMode  bool          `yaml:"cluster_mode"`
	ClusterAddrs []string      `yaml:"cluster_addrs"`
}

type MySQLConfig struct {
	DSN             string        `yaml:"dsn"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
	Path    string `yaml:"path"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			GRPCAddr: ":9090",
			HTTPAddr: ":8080",
		},
		Scheduler: SchedulerConfig{
			LeaseDuration:     15 * time.Second,
			CronCheckInterval: time.Second,
			CronBatchSize:     100,
		},
		Worker: WorkerConfig{
			Queues:            []string{"default"},
			Concurrency:       100,
			HeartbeatInterval: 5 * time.Second,
			ShutdownTimeout:   30 * time.Second,
			TaskTimeout:       5 * time.Minute,
		},
		Etcd: EtcdConfig{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		},
		Redis: RedisConfig{
			Addr:         "localhost:6379",
			PoolSize:     100,
			MinIdleConns: 10,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		},
		MySQL: MySQLConfig{
			DSN:             "root:@tcp(localhost:3306)/dispatchhub?charset=utf8mb4&parseTime=true&loc=Local",
			MaxOpenConns:    50,
			MaxIdleConns:    10,
			ConnMaxLifetime: time.Hour,
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Addr:    ":9091",
			Path:    "/metrics",
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
	}
}

// LoadFromFile reads config from a YAML file and merges with defaults.
func LoadFromFile(path string) (*Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
