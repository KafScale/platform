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
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestJSONSchemaValidation(t *testing.T) {
	validator := &jsonRegistry{
		mode:    ModeLenient,
		baseURL: "http://schemas.example.com",
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path != "/orders.json" {
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Body:       io.NopCloser(bytes.NewReader(nil)),
					}, nil
				}
				body := []byte(`{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}`)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(body)),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
				}, nil
			}),
		},
		cacheTTL: time.Minute,
		compiled: map[string]cachedSchema{},
	}

	if err := validator.Validate(context.Background(), "orders", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("expected valid payload: %v", err)
	}

	if err := validator.Validate(context.Background(), "orders", []byte(`{"id":"bad"}`)); err == nil {
		t.Fatalf("expected invalid payload")
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
