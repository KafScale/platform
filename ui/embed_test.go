// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package ui

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStaticHandler(t *testing.T) {
	handler, err := StaticHandler()
	if err != nil {
		t.Fatalf("StaticHandler() error: %v", err)
	}
	if handler == nil {
		t.Fatal("StaticHandler() returned nil handler")
	}

	// Serve the root (index.html)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for /, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct == "" {
		t.Fatal("expected Content-Type header")
	}

	// Serve CSS file
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/style.css", nil)
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 for /style.css, got %d", rec2.Code)
	}
}

func TestStaticHandlerNotFound(t *testing.T) {
	handler, err := StaticHandler()
	if err != nil {
		t.Fatalf("StaticHandler() error: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nonexistent.xyz", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}
