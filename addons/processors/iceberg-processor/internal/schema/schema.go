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

package schema

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/KafScale/platform/addons/processors/iceberg-processor/internal/config"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// Mode determines how validation failures are handled.
type Mode string

const (
	ModeOff     Mode = "off"
	ModeLenient Mode = "lenient"
	ModeStrict  Mode = "strict"
)

// Validator validates payloads by topic.
type Validator interface {
	Mode() Mode
	Validate(ctx context.Context, topic string, payload []byte) error
}

func New(cfg config.Config) (Validator, error) {
	mode := Mode(strings.ToLower(cfg.Schema.Mode))
	switch mode {
	case ModeOff:
		return nil, nil
	case ModeLenient, ModeStrict:
		// ok
	default:
		return nil, fmt.Errorf("unsupported schema.mode %q", cfg.Schema.Mode)
	}

	if cfg.Schema.Registry.BaseURL == "" {
		return nil, fmt.Errorf("schema.registry.base_url is required")
	}

	timeout := time.Duration(cfg.Schema.Registry.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	cacheTTL := time.Duration(cfg.Schema.Registry.CacheSeconds) * time.Second
	if cacheTTL == 0 {
		cacheTTL = 5 * time.Minute
	}

	return &jsonRegistry{
		mode:     mode,
		baseURL:  strings.TrimRight(cfg.Schema.Registry.BaseURL, "/"),
		client:   &http.Client{Timeout: timeout},
		cacheTTL: cacheTTL,
		compiled: map[string]cachedSchema{},
	}, nil
}

type cachedSchema struct {
	schema  *jsonschema.Schema
	expires time.Time
}

type jsonRegistry struct {
	mode     Mode
	baseURL  string
	client   *http.Client
	cacheTTL time.Duration

	mu       sync.Mutex
	compiled map[string]cachedSchema
}

func (r *jsonRegistry) Mode() Mode {
	return r.mode
}

func (r *jsonRegistry) Validate(ctx context.Context, topic string, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	schema, err := r.getSchema(ctx, topic)
	if err != nil {
		return err
	}

	var value interface{}
	if err := json.Unmarshal(payload, &value); err != nil {
		return fmt.Errorf("invalid json payload: %w", err)
	}

	if err := schema.Validate(value); err != nil {
		return err
	}
	return nil
}

func (r *jsonRegistry) getSchema(ctx context.Context, topic string) (*jsonschema.Schema, error) {
	now := time.Now()
	r.mu.Lock()
	entry, ok := r.compiled[topic]
	if ok && entry.expires.After(now) {
		r.mu.Unlock()
		return entry.schema, nil
	}
	r.mu.Unlock()

	url := r.schemaURL(topic)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.New("schema not found")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("schema registry status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	compiler := jsonschema.NewCompiler()
	compiler.LoadURL = func(url string) (io.ReadCloser, error) {
		if url != "schema.json" {
			return nil, fmt.Errorf("unsupported schema url %q", url)
		}
		return io.NopCloser(strings.NewReader(string(body))), nil
	}

	if err := compiler.AddResource("schema.json", strings.NewReader(string(body))); err != nil {
		return nil, err
	}
	schema, err := compiler.Compile("schema.json")
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.compiled[topic] = cachedSchema{schema: schema, expires: now.Add(r.cacheTTL)}
	r.mu.Unlock()

	return schema, nil
}

func (r *jsonRegistry) schemaURL(topic string) string {
	return r.baseURL + "/" + path.Clean(topic) + ".json"
}
