package config

import (
	"os"
	"path/filepath"
	"strings"

	"P2P-CDN/pkg/protocol"
	"github.com/spf13/viper"
)

type ClientConfig struct {
	Client  ClientSettings  `mapstructure:"client"`
	Tracker TrackerSettings `mapstructure:"tracker"`
	P2P     P2PSettings     `mapstructure:"p2p"`
	Storage StorageSettings `mapstructure:"storage"`
	API     APISettings     `mapstructure:"api"`
	Logging LoggingSettings `mapstructure:"logging"`
}

type ClientSettings struct {
	ListenPort int    `mapstructure:"listen_port"`
	DataDir    string `mapstructure:"data_dir"`
}

type TrackerSettings struct {
	URL string `mapstructure:"url"`
}

type P2PSettings struct {
	EnableRelay     bool     `mapstructure:"enable_relay"`
	EnableHolePunch bool     `mapstructure:"enable_hole_punch"`
	BootstrapPeers  []string `mapstructure:"bootstrap_peers"`
	ExternalIP      string   `mapstructure:"external_ip"`
}

type StorageSettings struct {
	ChunkSize      uint32 `mapstructure:"chunk_size"`
	BlockstoreType string `mapstructure:"blockstore_type"`
}

type APISettings struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

type LoggingSettings struct {
	Level string `mapstructure:"level"`
	File  string `mapstructure:"file"`
}

type TrackerConfig struct {
	Tracker  TrackerServerSettings `mapstructure:"tracker"`
	Database DatabaseSettings      `mapstructure:"database"`
	Redis    RedisSettings         `mapstructure:"redis"`
	Logging  LoggingSettings       `mapstructure:"logging"`
}

type TrackerServerSettings struct {
	Port         int               `mapstructure:"port"`
	RelayServers []string          `mapstructure:"relay_servers"`
	RateLimit    RateLimitSettings `mapstructure:"rate_limit"`
}

type RateLimitSettings struct {
	RequestsPerMinute int `mapstructure:"requests_per_minute"`
}

type DatabaseSettings struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	SSLMode  string `mapstructure:"sslmode"`
}

type RedisSettings struct {
	URL      string `mapstructure:"url"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

func LoadClientConfig(configPath string) (*ClientConfig, error) {
	v := viper.New()

	setClientDefaults(v)

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		home, err := os.UserHomeDir()
		if err == nil {
			v.AddConfigPath(filepath.Join(home, ".p2pcdn"))
		}
		v.AddConfigPath("./configs")
		v.AddConfigPath(".")
		v.SetConfigName("client")
		v.SetConfigType("yaml")
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	v.SetEnvPrefix("P2PCDN")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var config ClientConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	config.Client.DataDir = expandHome(config.Client.DataDir)
	config.Logging.File = expandHome(config.Logging.File)

	return &config, nil
}

func LoadTrackerConfig(configPath string) (*TrackerConfig, error) {
	v := viper.New()

	setTrackerDefaults(v)

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.AddConfigPath("./configs")
		v.AddConfigPath(".")
		v.SetConfigName("tracker")
		v.SetConfigType("yaml")
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	v.SetEnvPrefix("P2PCDN")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var config TrackerConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

func setClientDefaults(v *viper.Viper) {
	v.SetDefault("client.listen_port", protocol.DefaultListenPort)
	v.SetDefault("client.data_dir", protocol.DefaultDataDir)
	v.SetDefault("tracker.url", protocol.DefaultTrackerURL)
	v.SetDefault("p2p.enable_relay", true)
	v.SetDefault("p2p.enable_hole_punch", true)
	v.SetDefault("p2p.bootstrap_peers", protocol.DefaultBootstrapPeers)
	v.SetDefault("p2p.external_ip", "")
	v.SetDefault("storage.chunk_size", protocol.DefaultChunkSize)
	v.SetDefault("storage.blockstore_type", protocol.BlockstoreType)
	v.SetDefault("api.enabled", false)
	v.SetDefault("api.port", protocol.DefaultAPIPort)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.file", protocol.DefaultLogFile)
}

func setTrackerDefaults(v *viper.Viper) {
	v.SetDefault("tracker.port", protocol.DefaultTrackerPort)
	v.SetDefault("tracker.rate_limit.requests_per_minute", protocol.TrackerRateLimit)
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 3306)
	v.SetDefault("database.user", "p2pcdn")
	v.SetDefault("database.password", "p2pcdn123")
	v.SetDefault("database.database", "p2pcdn")
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("redis.url", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("logging.level", "info")
}

func expandHome(path string) string {
	if path == "" || path[0] != '~' {
		return path
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return path
	}

	return filepath.Join(home, path[1:])
}
