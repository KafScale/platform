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

package health

import (
	"context"
	"net/http"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
)

const readinessTimeout = time.Second

type readinessChecker interface {
	CheckReadiness(context.Context) error
}

// Register keeps process liveness independent from metadata readiness. Services
// without a metadata store can still serve their standalone/metrics-only mode.
func Register(mux *http.ServeMux, store metadata.Store) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := CheckMetadata(r.Context(), store); err != nil {
			http.Error(w, "not ready: metadata unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

// CheckMetadata checks live metadata access with a one-second deadline. Stores
// without a live readiness capability use their normal metadata read.
func CheckMetadata(ctx context.Context, store metadata.Store) error {
	if store == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	if checker, ok := store.(readinessChecker); ok {
		return checker.CheckReadiness(ctx)
	}
	_, err := store.Metadata(ctx, nil)
	return err
}
