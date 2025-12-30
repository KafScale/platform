package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config defines the processor configuration schema.
type Config struct {
	S3        S3Config        `yaml:"s3"`
	Iceberg   IcebergConfig   `yaml:"iceberg"`
	Discovery DiscoveryConfig `yaml:"discovery"`
	Etcd      EtcdConfig      `yaml:"etcd"`
	Schema    SchemaConfig    `yaml:"schema"`
	Mappings  []Mapping       `yaml:"mappings"`
	Offsets   OffsetConfig    `yaml:"offsets"`
	Processor ProcessorConfig `yaml:"processor"`
}

type S3Config struct {
	Bucket    string `yaml:"bucket"`
	Namespace string `yaml:"namespace"`
	Endpoint  string `yaml:"endpoint"`
	Region    string `yaml:"region"`
	PathStyle bool   `yaml:"path_style"`
}

type OffsetConfig struct {
	Backend         string `yaml:"backend"`
	LeaseTTLSeconds int    `yaml:"lease_ttl_seconds"`
	KeyPrefix       string `yaml:"key_prefix"`
}

type ProcessorConfig struct {
	PollIntervalSeconds int `yaml:"poll_interval_seconds"`
}

type DiscoveryConfig struct {
	Mode string `yaml:"mode"`
}

type EtcdConfig struct {
	Endpoints []string `yaml:"endpoints"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
}

type SchemaConfig struct {
	Mode     string         `yaml:"mode"`
	Registry RegistryConfig `yaml:"registry"`
}

type RegistryConfig struct {
	BaseURL        string `yaml:"base_url"`
	TimeoutSeconds int    `yaml:"timeout_seconds"`
	CacheSeconds   int    `yaml:"cache_seconds"`
}

type Mapping struct {
	Topic               string `yaml:"topic"`
	Table               string `yaml:"table"`
	Mode                string `yaml:"mode"`
	CreateTableIfAbsent bool   `yaml:"create_table_if_missing"`
	Schema              MappingSchemaConfig `yaml:"schema"`
}

type MappingSchemaConfig struct {
	Source            string   `yaml:"source"`
	Columns           []Column `yaml:"columns"`
	AllowTypeWidening bool     `yaml:"allow_type_widening"`
}

type Column struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Required bool   `yaml:"required"`
}

type IcebergConfig struct {
	Catalog   CatalogConfig `yaml:"catalog"`
	Warehouse string        `yaml:"warehouse"`
}

type CatalogConfig struct {
	Type     string `yaml:"type"`
	URI      string `yaml:"uri"`
	Token    string `yaml:"token"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	if cfg.S3.Bucket == "" {
		return Config{}, fmt.Errorf("s3.bucket is required")
	}
	if cfg.Iceberg.Catalog.Type == "" {
		return Config{}, fmt.Errorf("iceberg.catalog.type is required")
	}
	if cfg.Iceberg.Catalog.URI == "" {
		return Config{}, fmt.Errorf("iceberg.catalog.uri is required")
	}
	if cfg.Discovery.Mode == "" {
		cfg.Discovery.Mode = "auto"
	}
	if cfg.Schema.Mode == "" {
		cfg.Schema.Mode = "off"
	}
	if cfg.Offsets.Backend == "" {
		cfg.Offsets.Backend = "etcd"
	}
	if cfg.Offsets.LeaseTTLSeconds == 0 {
		cfg.Offsets.LeaseTTLSeconds = 30
	}
	if cfg.Offsets.KeyPrefix == "" {
		cfg.Offsets.KeyPrefix = "processors"
	}
	if cfg.Processor.PollIntervalSeconds == 0 {
		cfg.Processor.PollIntervalSeconds = 5
	}
	if cfg.Schema.Mode != "off" && cfg.Schema.Registry.BaseURL == "" {
		return Config{}, fmt.Errorf("schema.registry.base_url is required when schema.mode is enabled")
	}
	if cfg.Offsets.Backend == "etcd" && len(cfg.Etcd.Endpoints) == 0 {
		return Config{}, fmt.Errorf("etcd.endpoints is required for offsets.backend=etcd")
	}
	for i, mapping := range cfg.Mappings {
		if mapping.Topic == "" {
			return Config{}, fmt.Errorf("mappings[%d].topic is required", i)
		}
		if mapping.Table == "" {
			return Config{}, fmt.Errorf("mappings[%d].table is required", i)
		}
		if mapping.Mode == "" {
			mapping.Mode = "append"
		}
		if mapping.Mode != "append" {
			return Config{}, fmt.Errorf("mappings[%d].mode must be append", i)
		}
		if mapping.Schema.Source == "" {
			if len(mapping.Schema.Columns) > 0 {
				mapping.Schema.Source = "mapping"
			} else if cfg.Schema.Registry.BaseURL != "" {
				mapping.Schema.Source = "registry"
			} else {
				mapping.Schema.Source = "none"
			}
		}
		switch mapping.Schema.Source {
		case "mapping":
			if len(mapping.Schema.Columns) == 0 {
				return Config{}, fmt.Errorf("mappings[%d].schema.columns is required for schema.source=mapping", i)
			}
			for cIdx, col := range mapping.Schema.Columns {
				if col.Name == "" {
					return Config{}, fmt.Errorf("mappings[%d].schema.columns[%d].name is required", i, cIdx)
				}
				if col.Type == "" {
					return Config{}, fmt.Errorf("mappings[%d].schema.columns[%d].type is required", i, cIdx)
				}
				if !isSupportedColumnType(col.Type) {
					return Config{}, fmt.Errorf("mappings[%d].schema.columns[%d].type %q is not supported", i, cIdx, col.Type)
				}
			}
		case "registry":
			if cfg.Schema.Registry.BaseURL == "" {
				return Config{}, fmt.Errorf("schema.registry.base_url is required for schema.source=registry")
			}
		case "none":
		default:
			return Config{}, fmt.Errorf("mappings[%d].schema.source %q is not supported", i, mapping.Schema.Source)
		}
		cfg.Mappings[i] = mapping
	}

	return cfg, nil
}

func isSupportedColumnType(value string) bool {
	switch strings.ToLower(value) {
	case "boolean", "int", "long", "float", "double", "string", "binary", "timestamp", "date":
		return true
	default:
		return false
	}
}
