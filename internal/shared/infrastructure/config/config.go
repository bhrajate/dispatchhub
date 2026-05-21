package config

import (
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// 所有服务共用的基础设施配置类型。

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
	// ConnMaxIdleTime 限制 idle connection 在关闭前可存活的时长。
	// 可选项，0（默认值）表示永久保留 idle connection，
	// 这可能让中间设备或 MySQL 自身残留 stale TCP 状态。
	// 生产环境推荐 5-30 分钟，短于服务端 wait_timeout。
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

// Active 表示是否应接入 limiter。rate 为 0 表示“不限流”，
// 调用方应完全跳过 limiter 构造。
func (c RateLimitConfig) Active() bool {
	return c.Enabled && c.DefaultRate > 0 && c.DefaultBurst > 0
}

// 基础设施配置的共享默认值。

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

// LoadYAML 读取 YAML 文件并反序列化到目标结构体。
func LoadYAML(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, target)
}

// ApplyEnvOverrides 使用环境变量覆盖基础设施配置字段。

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
