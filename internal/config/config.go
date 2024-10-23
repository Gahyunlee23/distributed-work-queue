package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Queue   QueueConfig   `yaml:"queue"`
	Storage StorageConfig `yaml:"storage"`
	Logger  LoggerConfig  `yaml:"logger"`
}

type ServerConfig struct {
	Port int    `yaml:"port"`
	Host string `yaml:"host"`
}

type QueueConfig struct {
	WorkerCount int `yaml:"worker_count"`
	BufferSize  int `yaml:"buffer_size"`
	RetryLimit  int `yaml:"retry_limit"`
	RetryDelay  int `yaml:"retry_delay"`
}

type StorageConfig struct {
	Type  string      `yaml:"type"`
	Redis RedisConfig `yaml:"redis"`
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type LoggerConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func LoadConfig(filePath string) (*Config, error) {
	config := &Config{
		Server: ServerConfig{
			Port: 8080,
			Host: "localhost",
		},
		Queue: QueueConfig{
			WorkerCount: 5,
			BufferSize:  100,
			RetryLimit:  3,
			RetryDelay:  60,
		},
		Storage: StorageConfig{
			Type: "memory",
		},
		Logger: LoggerConfig{
			Level:  "info",
			Format: "json",
		},
	}

	if err := config.loadFile(filePath); err != nil {
		return nil, fmt.Errorf("load config error: %w", err)
	}

	config.loadEnv()

	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("validate config error: %w", err)
	}

	return config, nil
}

func (c *Config) loadFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file error: %w", err)
	}
	return yaml.Unmarshal(data, c)
}

func (c *Config) loadEnv() {
	if port := os.Getenv("SERVER_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err != nil {
			c.Server.Port = p
		}
	}

	if host := os.Getenv("SERVER_HOST"); host != "" {
		c.Server.Host = host
	}
	// set the queue up
	if workers := os.Getenv("SERVER_WORKERS"); workers != "" {
		if w, err := strconv.Atoi(workers); err != nil {
			c.Queue.WorkerCount = w
		}
	}

	if redisHost := os.Getenv("REDIS_HOST"); redisHost != "" {
		c.Storage.Type = "redis"
		c.Storage.Redis.Host = redisHost
	}
}

// validate the value
func (c *Config) validate() error {
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	if c.Queue.WorkerCount < 1 {
		return fmt.Errorf("invalid worker count: %d", c.Queue.WorkerCount)
	}

	if c.Storage.Type != "redis" && c.Storage.Type != "memory" {
		return fmt.Errorf("invalid storage type: %s", c.Storage.Type)
	}

	return nil
}
