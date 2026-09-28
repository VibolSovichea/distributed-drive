


package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)



const EnvPrefix = "DD_"


type HTTPConfig struct {
	Addr              string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	ReadHeaderTimeout time.Duration
}


type DatabaseConfig struct {
	Path          string
	BusyTimeout   time.Duration
	MaxOpenConns  int
	MigrateOnOpen bool
}


type StorageConfig struct {
	
	
	
	TokenDir string

	
	
	
	
	
	TokenEncryptionKey string

	
	
	
	GoogleClientID     string
	GoogleClientSecret string
	
	GoogleRedirectURI string

	
	
	
	LocalNodeRoot string
}






func (s StorageConfig) GoogleEnabled() bool {
	return strings.TrimSpace(s.TokenDir) != "" &&
		strings.TrimSpace(s.TokenEncryptionKey) != "" &&
		strings.TrimSpace(s.GoogleClientID) != "" &&
		strings.TrimSpace(s.GoogleClientSecret) != ""
}


func (s StorageConfig) AnyProviderEnabled() bool {
	return s.GoogleEnabled() || strings.TrimSpace(s.LocalNodeRoot) != ""
}


type LoggingConfig struct {
	Level  string
	Format string
}


type Config struct {
	HTTP     HTTPConfig
	Database DatabaseConfig
	Storage  StorageConfig
	Logging  LoggingConfig
}




func Defaults() Config {
	return Config{
		HTTP: HTTPConfig{
			Addr:              ":8080",
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      0, 
			IdleTimeout:       120 * time.Second,
			ShutdownTimeout:   15 * time.Second,
			ReadHeaderTimeout: 10 * time.Second,
		},
		Database: DatabaseConfig{
			Path:          filepath.Join("data", "distributed-drive.db"),
			BusyTimeout:   5 * time.Second,
			MaxOpenConns:  1,
			MigrateOnOpen: true,
		},
		Storage: StorageConfig{
			
			
			TokenDir: filepath.Join("data", "tokens"),
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
	}
}



func Load() (Config, error) {
	return loadFrom(os.LookupEnv)
}


type lookupFunc func(key string) (string, bool)

func loadFrom(lookup lookupFunc) (Config, error) {
	cfg := Defaults()

	var errs []error

	str := func(key string, dst *string) {
		if v, ok := lookup(EnvPrefix + key); ok {
			*dst = strings.TrimSpace(v)
		}
	}
	dur := func(key string, dst *time.Duration) {
		if v, ok := lookup(EnvPrefix + key); ok {
			parsed, err := parseDuration(key, v)
			if err != nil {
				errs = append(errs, err)
				return
			}
			*dst = parsed
		}
	}
	boolean := func(key string, dst *bool) {
		if v, ok := lookup(EnvPrefix + key); ok {
			parsed, err := parseBool(key, v)
			if err != nil {
				errs = append(errs, err)
				return
			}
			*dst = parsed
		}
	}
	positiveInt := func(key string, dst *int) {
		if v, ok := lookup(EnvPrefix + key); ok {
			parsed, err := parsePositiveInt(key, v)
			if err != nil {
				errs = append(errs, err)
				return
			}
			*dst = parsed
		}
	}

	str("HTTP_ADDR", &cfg.HTTP.Addr)
	dur("HTTP_READ_TIMEOUT", &cfg.HTTP.ReadTimeout)
	dur("HTTP_WRITE_TIMEOUT", &cfg.HTTP.WriteTimeout)
	dur("HTTP_IDLE_TIMEOUT", &cfg.HTTP.IdleTimeout)
	dur("HTTP_SHUTDOWN_TIMEOUT", &cfg.HTTP.ShutdownTimeout)

	str("DATABASE_PATH", &cfg.Database.Path)
	dur("DATABASE_BUSY_TIMEOUT", &cfg.Database.BusyTimeout)
	positiveInt("DATABASE_MAX_OPEN_CONNS", &cfg.Database.MaxOpenConns)
	boolean("MIGRATE_ON_START", &cfg.Database.MigrateOnOpen)

	str("STORAGE_TOKEN_DIR", &cfg.Storage.TokenDir)
	str("STORAGE_TOKEN_KEY", &cfg.Storage.TokenEncryptionKey)
	str("STORAGE_GOOGLE_CLIENT_ID", &cfg.Storage.GoogleClientID)
	str("STORAGE_GOOGLE_CLIENT_SECRET", &cfg.Storage.GoogleClientSecret)
	str("STORAGE_GOOGLE_REDIRECT_URI", &cfg.Storage.GoogleRedirectURI)
	str("STORAGE_LOCAL_NODE_ROOT", &cfg.Storage.LocalNodeRoot)

	str("LOG_LEVEL", &cfg.Logging.Level)
	str("LOG_FORMAT", &cfg.Logging.Format)

	errs = append(errs, cfg.validate()...)

	return cfg, errors.Join(errs...)
}

func (c Config) validate() []error {
	var errs []error

	if c.HTTP.Addr == "" {
		errs = append(errs, fmt.Errorf("%sHTTP_ADDR: must not be empty", EnvPrefix))
	}
	if c.HTTP.ReadTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%sHTTP_READ_TIMEOUT: must be positive", EnvPrefix))
	}
	if c.HTTP.IdleTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%sHTTP_IDLE_TIMEOUT: must be positive", EnvPrefix))
	}
	if c.HTTP.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%sHTTP_SHUTDOWN_TIMEOUT: must be positive", EnvPrefix))
	}
	if c.HTTP.WriteTimeout < 0 {
		errs = append(errs, fmt.Errorf("%sHTTP_WRITE_TIMEOUT: must not be negative", EnvPrefix))
	}
	if c.Database.Path == "" {
		errs = append(errs, fmt.Errorf("%sDATABASE_PATH: must not be empty", EnvPrefix))
	}
	if c.Database.BusyTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%sDATABASE_BUSY_TIMEOUT: must be positive", EnvPrefix))
	}
	if c.Database.MaxOpenConns < 1 {
		errs = append(errs, fmt.Errorf("%sDATABASE_MAX_OPEN_CONNS: must be at least 1", EnvPrefix))
	}
	switch c.Logging.Level {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("%sLOG_LEVEL: %q is not one of debug, info, warn, error",
			EnvPrefix, c.Logging.Level))
	}
	switch c.Logging.Format {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("%sLOG_FORMAT: %q is not one of json, text",
			EnvPrefix, c.Logging.Format))
	}

	return errs
}



func (c Config) String() string {
	
	return fmt.Sprintf(
		"http{addr:%s read_timeout:%s write_timeout:%s idle_timeout:%s shutdown_timeout:%s} "+
			"database{path:%s busy_timeout:%s max_open_conns:%d migrate_on_open:%t} "+
			"storage{token_dir:%s token_key:%t google:%t google_redirect:%q local_root:%q} "+
			"logging{level:%s format:%s}",
		c.HTTP.Addr, c.HTTP.ReadTimeout, c.HTTP.WriteTimeout,
		c.HTTP.IdleTimeout, c.HTTP.ShutdownTimeout,
		c.Database.Path, c.Database.BusyTimeout, c.Database.MaxOpenConns, c.Database.MigrateOnOpen,
		c.Storage.TokenDir, strings.TrimSpace(c.Storage.TokenEncryptionKey) != "",
		c.Storage.GoogleEnabled(), c.Storage.GoogleRedirectURI, c.Storage.LocalNodeRoot,
		c.Logging.Level, c.Logging.Format,
	)
}

func parseDuration(key, raw string) (time.Duration, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return 0, fmt.Errorf("%s%s: must not be empty", EnvPrefix, key)
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s%s: %q is not a valid duration: %w", EnvPrefix, key, v, err)
	}
	return d, nil
}

func parseBool(key, raw string) (bool, error) {
	v := strings.TrimSpace(raw)
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s%s: %q is not a valid boolean: %w", EnvPrefix, key, v, err)
	}
	return b, nil
}

func parsePositiveInt(key, raw string) (int, error) {
	v := strings.TrimSpace(raw)
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s%s: %q is not an integer: %w", EnvPrefix, key, v, err)
	}
	if n < 1 {
		return 0, fmt.Errorf("%s%s: must be at least 1, got %d", EnvPrefix, key, n)
	}
	return n, nil
}
