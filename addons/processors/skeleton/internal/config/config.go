// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config defines the processor configuration schema.
type Config struct {
	S3       S3Config     `yaml:"s3"`
	Mappings []Mapping    `yaml:"mappings"`
	Offsets  OffsetConfig `yaml:"offsets"`
}

type S3Config struct {
	Bucket    string `yaml:"bucket"`
	Namespace string `yaml:"namespace"`
	Endpoint  string `yaml:"endpoint"`
	Region    string `yaml:"region"`
}

type OffsetConfig struct {
	Backend string `yaml:"backend"`
}

type Mapping struct {
	Topic               string `yaml:"topic"`
	Sink                string `yaml:"sink"`
	Mode                string `yaml:"mode"`
	CreateTableIfAbsent bool   `yaml:"create_table_if_missing"`
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

	return cfg, nil
}
