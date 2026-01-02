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
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\n  namespace: dev\niceberg:\n  catalog:\n    type: rest\n    uri: http://catalog\ndiscovery:\n  mode: auto\netcd:\n  endpoints:\n    - http://etcd:2379\nschema:\n  mode: \"off\"\nmappings:\n  - topic: orders\n    table: prod.orders\n")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.S3.Bucket != "test-bucket" {
		t.Fatalf("unexpected bucket: %s", cfg.S3.Bucket)
	}
}

func TestLoadDefaultsOffsetsBackend(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\niceberg:\n  catalog:\n    type: rest\n    uri: http://catalog\netcd:\n  endpoints:\n    - http://etcd:2379\nschema:\n  mode: \"off\"\nmappings:\n  - topic: orders\n    table: prod.orders\n")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Offsets.Backend != "etcd" {
		t.Fatalf("expected offsets backend etcd, got %q", cfg.Offsets.Backend)
	}
	if cfg.Offsets.LeaseTTLSeconds != 30 {
		t.Fatalf("expected default lease ttl 30, got %d", cfg.Offsets.LeaseTTLSeconds)
	}
	if cfg.Offsets.KeyPrefix != "processors" {
		t.Fatalf("expected default key prefix processors, got %q", cfg.Offsets.KeyPrefix)
	}
	if cfg.Processor.PollIntervalSeconds != 5 {
		t.Fatalf("expected default poll interval 5, got %d", cfg.Processor.PollIntervalSeconds)
	}
	if cfg.Mappings[0].Mode != "append" {
		t.Fatalf("expected default mapping mode append, got %q", cfg.Mappings[0].Mode)
	}
}

func TestLoadRequiresEtcdEndpointsForOffsets(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\niceberg:\n  catalog:\n    type: rest\n    uri: http://catalog\nschema:\n  mode: \"off\"\nmappings:\n  - topic: orders\n    table: prod.orders\n")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatalf("expected error when etcd endpoints are missing")
	}
}

func TestLoadRejectsInvalidMappingMode(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\niceberg:\n  catalog:\n    type: rest\n    uri: http://catalog\netcd:\n  endpoints:\n    - http://etcd:2379\nschema:\n  mode: \"off\"\nmappings:\n  - topic: orders\n    table: prod.orders\n    mode: overwrite\n")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatalf("expected error for invalid mapping mode")
	}
}

func TestLoadDefaultsSchemaSourceFromColumns(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\niceberg:\n  catalog:\n    type: rest\n    uri: http://catalog\netcd:\n  endpoints:\n    - http://etcd:2379\nschema:\n  mode: \"off\"\nmappings:\n  - topic: orders\n    table: prod.orders\n    schema:\n      columns:\n        - name: order_id\n          type: long\n")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Mappings[0].Schema.Source != "mapping" {
		t.Fatalf("expected schema source mapping, got %q", cfg.Mappings[0].Schema.Source)
	}
}

func TestLoadDefaultsSchemaSourceFromRegistry(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\niceberg:\n  catalog:\n    type: rest\n    uri: http://catalog\netcd:\n  endpoints:\n    - http://etcd:2379\nschema:\n  mode: \"off\"\n  registry:\n    base_url: http://schemas\nmappings:\n  - topic: orders\n    table: prod.orders\n")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Mappings[0].Schema.Source != "registry" {
		t.Fatalf("expected schema source registry, got %q", cfg.Mappings[0].Schema.Source)
	}
}

func TestLoadRejectsInvalidSchemaColumnType(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\niceberg:\n  catalog:\n    type: rest\n    uri: http://catalog\netcd:\n  endpoints:\n    - http://etcd:2379\nschema:\n  mode: \"off\"\nmappings:\n  - topic: orders\n    table: prod.orders\n    schema:\n      columns:\n        - name: order_id\n          type: uuid\n")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatalf("expected error for invalid column type")
	}
}

func TestLoadRejectsRegistrySourceWithoutBaseURL(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\niceberg:\n  catalog:\n    type: rest\n    uri: http://catalog\netcd:\n  endpoints:\n    - http://etcd:2379\nschema:\n  mode: \"off\"\nmappings:\n  - topic: orders\n    table: prod.orders\n    schema:\n      source: registry\n")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatalf("expected error for schema.source=registry without base_url")
	}
}
