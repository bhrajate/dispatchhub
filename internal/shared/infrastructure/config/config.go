package config

import (
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Shared infrastructure config types used by all services.

type ServerConfig struct {
	GRPCAddr string `yaml:"grpc_addr" env:"DISPATCH_GRPC_ADDR"`
	HTTPAddr string `yaml:"http_addr" env:"DISPATCH_HTTP_ADDR"`
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
	// ConnMaxIdleTime caps how long an idle connection may live before being
	// closed. Optional — zero (default) keeps idle connections forever, which
	// can leave middleboxes / MySQL itself with stale TCP state. Recommended
	// 5-30 minutes in production, shorter than wait_timeout on the server.
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
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

type RateLimitConfig struct {
	Enabled      bool                          `yaml:"enabled"`
	DefaultRate  float64                       `yaml:"default_rate"`
	DefaultBurst int                           `yaml:"default_burst"`
	PerQueue     map[string]QueueRateLimitSpec `yaml:"per_queue"`
}

type QueueRateLimitSpec struct {
	Rate  float64 `yaml:"rate"`
	Burst int     `yaml:"burst"`
}

// Active reports whether the limiter should be wired in. A zero rate means
// "no limit" — callers should skip building the limiter entirely.
func (c RateLimitConfig) Active() bool {
	return c.Enabled && c.DefaultRate > 0 && c.DefaultBurst > 0
}

// Shared defaults for infrastructure configs.

func DefaultEtcdConfig() EtcdConfig {
	return EtcdConfig{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}
}

func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		Addr:         "localhost:6379",
		PoolSize:     100,
		MinIdleConns: 10,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

func DefaultMySQLConfig() MySQLConfig {
	return MySQLConfig{
		DSN:             "root:@tcp(localhost:3306)/dispatchhub?charset=utf8mb4&parseTime=true&loc=Local",
		MaxOpenConns:    50,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
	}
}

func DefaultMetricsConfig() MetricsConfig {
	return MetricsConfig{
		Enabled: true,
		Addr:    ":9091",
		Path:    "/metrics",
	}
}

func DefaultLogConfig() LogConfig {
	return LogConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	}
}

func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		Enabled:      true,
		DefaultRate:  1000,
		DefaultBurst: 1000,
	}
}

// LoadYAML reads a YAML file and unmarshals it into the target struct.
func LoadYAML(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, target)
}

// ApplyEnvOverrides overrides infrastructure config fields from environment variables.

func ApplyServerEnvOverrides(cfg *ServerConfig) {
	if v := os.Getenv("DISPATCH_GRPC_ADDR"); v != "" {
		cfg.GRPCAddr = v
	}
	if v := os.Getenv("DISPATCH_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
}

func ApplyRedisEnvOverrides(cfg *RedisConfig) {
	if v := os.Getenv("DISPATCH_REDIS_ADDR"); v != "" {
		cfg.Addr = v
	}
	if v := os.Getenv("DISPATCH_REDIS_PASSWORD"); v != "" {
		cfg.Password = v
	}
}

func ApplyMySQLEnvOverrides(cfg *MySQLConfig) {
	if v := os.Getenv("DISPATCH_MYSQL_DSN"); v != "" {
		cfg.DSN = v
	}
}

func ApplyEtcdEnvOverrides(cfg *EtcdConfig) {
	if v := os.Getenv("DISPATCH_ETCD_ENDPOINTS"); v != "" {
		cfg.Endpoints = strings.Split(v, ",")
	}
}

func ApplyLogEnvOverrides(cfg *LogConfig) {
	if v := os.Getenv("DISPATCH_LOG_LEVEL"); v != "" {
		cfg.Level = v
	}
}
