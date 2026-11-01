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

package schemaregistry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	contentType  = "application/vnd.schemaregistry.v1+json"
	maxBodyBytes = 16 << 20
)

type Server struct {
	reg      *Registry
	timeout  time.Duration
	mux      *http.ServeMux
	requests *prometheus.CounterVec
}

type schemaBody struct {
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType,omitempty"`
	References []Reference `json:"references,omitempty"`
	ID         int64       `json:"id,omitempty"`
	Version    int         `json:"version,omitempty"`
}

type versionBody struct {
	Subject    string      `json:"subject"`
	Version    int         `json:"version"`
	ID         int64       `json:"id"`
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType,omitempty"`
	References []Reference `json:"references,omitempty"`
}

type errorBody struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func NewServer(reg *Registry, timeout time.Duration) *Server {
	s := &Server{reg: reg, timeout: timeout, mux: http.NewServeMux()}
	metrics := prometheus.NewRegistry()
	s.requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafscale_schema_registry_requests_total",
		Help: "Schema registry HTTP requests by route and status code.",
	}, []string{"route", "code"})
	metrics.MustRegister(s.requests, prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "kafscale_schema_registry_txn_conflicts_total",
		Help: "Schema registry etcd transactions retried after a compare failed.",
	}, func() float64 { return float64(reg.Conflicts()) }))
	routes := map[string]func(http.ResponseWriter, *http.Request) (any, error){
		"GET /{$}":                                          func(http.ResponseWriter, *http.Request) (any, error) { return map[string]any{}, nil },
		"GET /schemas/types":                                func(http.ResponseWriter, *http.Request) (any, error) { return []string{schemaTypeAvro}, nil },
		"GET /schemas/ids/{id}":                             s.getSchema,
		"GET /schemas/ids/{id}/schema":                      s.getSchemaText,
		"GET /schemas/ids/{id}/versions":                    s.getIDVersions,
		"GET /subjects":                                     s.getSubjects,
		"GET /subjects/{subject}/versions":                  s.getVersions,
		"GET /subjects/{subject}/versions/{version}":        s.getVersion,
		"GET /subjects/{subject}/versions/{version}/schema": s.getVersionText,
		"POST /subjects/{subject}/versions":                 s.register,
		"POST /subjects/{subject}":                          s.lookup,
		"DELETE /subjects/{subject}":                        s.deleteSubject,
		"DELETE /subjects/{subject}/versions/{version}":     s.deleteVersion,
		"GET /config":                                       s.getConfig,
		"PUT /config":                                       s.putConfig,
		"GET /config/{subject}":                             s.getConfig,
		"PUT /config/{subject}":                             s.putConfig,
		"DELETE /config/{subject}":                          s.deleteConfig,
		"GET /mode":                                         s.getMode,
		"PUT /mode":                                         s.putMode,
		"GET /mode/{subject}":                               s.getMode,
		"PUT /mode/{subject}":                               s.putMode,
		"DELETE /mode/{subject}":                            s.deleteMode,
		"POST /compatibility/subjects/{subject}/versions/{version}": s.compatibility,
	}
	for pattern, fn := range routes {
		s.mux.Handle(pattern, s.wrap(fn))
	}
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(metrics, promhttp.HandlerOpts{}))
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	s.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := reg.Ready(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	s.mux.Handle("/", s.wrap(func(http.ResponseWriter, *http.Request) (any, error) {
		return nil, newError(404, 404, "HTTP 404 Not Found")
	}))
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) wrap(fn func(http.ResponseWriter, *http.Request) (any, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
		defer cancel()
		r = r.WithContext(ctx)
		status := http.StatusOK
		out, err := fn(w, r)
		w.Header().Set("Content-Type", contentType)
		if err != nil {
			var regErr *Error
			if !errors.As(err, &regErr) {
				regErr = errStore(err)
			}
			status = regErr.Status
			out = errorBody{ErrorCode: regErr.Code, Message: regErr.Message}
		}
		s.requests.WithLabelValues(r.Pattern, strconv.Itoa(status)).Inc()
		if text, ok := out.(rawText); ok {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, string(text))
			return
		}
		data, err := json.Marshal(out)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write(data)
	})
}

type rawText string

func flag(r *http.Request, name string) bool {
	return r.URL.Query().Get(name) == "true"
}

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, errSchemaNotFound()
	}
	return id, nil
}

func pathVersion(r *http.Request) (int, error) {
	return ParseVersion(r.PathValue("version"))
}

func decode(r *http.Request, v any) error {
	if err := json.NewDecoder(http.MaxBytesReader(nil, r.Body, maxBodyBytes)).Decode(v); err != nil {
		return errBadRequest("invalid request body: %v", err)
	}
	return nil
}

func typeName(t string) string {
	if t == schemaTypeAvro {
		return ""
	}
	return t
}

func versionResponse(v *VersionedSchema) versionBody {
	return versionBody{
		Subject:    v.Subject,
		Version:    v.Version,
		ID:         v.Schema.ID,
		Schema:     v.Schema.Text,
		SchemaType: typeName(v.Schema.Type),
		References: v.Schema.References,
	}
}

func (s *Server) getSchema(_ http.ResponseWriter, r *http.Request) (any, error) {
	id, err := pathID(r)
	if err != nil {
		return nil, err
	}
	sc, err := s.reg.SchemaByID(r.Context(), id)
	if err != nil {
		return nil, err
	}
	return schemaBody{Schema: sc.Text, SchemaType: typeName(sc.Type), References: sc.References}, nil
}

func (s *Server) getSchemaText(_ http.ResponseWriter, r *http.Request) (any, error) {
	id, err := pathID(r)
	if err != nil {
		return nil, err
	}
	sc, err := s.reg.SchemaByID(r.Context(), id)
	if err != nil {
		return nil, err
	}
	return rawText(sc.Text), nil
}

func (s *Server) getIDVersions(_ http.ResponseWriter, r *http.Request) (any, error) {
	id, err := pathID(r)
	if err != nil {
		return nil, err
	}
	return s.reg.IDVersions(r.Context(), id, flag(r, "deleted"))
}

func (s *Server) getSubjects(_ http.ResponseWriter, r *http.Request) (any, error) {
	return s.reg.Subjects(r.Context(), flag(r, "deleted"))
}

func (s *Server) getVersions(_ http.ResponseWriter, r *http.Request) (any, error) {
	return s.reg.Versions(r.Context(), r.PathValue("subject"), flag(r, "deleted"))
}

func (s *Server) versionFromPath(r *http.Request) (*VersionedSchema, error) {
	version, err := pathVersion(r)
	if err != nil {
		return nil, err
	}
	return s.reg.Version(r.Context(), r.PathValue("subject"), version, flag(r, "deleted"))
}

func (s *Server) getVersion(_ http.ResponseWriter, r *http.Request) (any, error) {
	v, err := s.versionFromPath(r)
	if err != nil {
		return nil, err
	}
	return versionResponse(v), nil
}

func (s *Server) getVersionText(_ http.ResponseWriter, r *http.Request) (any, error) {
	v, err := s.versionFromPath(r)
	if err != nil {
		return nil, err
	}
	return rawText(v.Schema.Text), nil
}

func registerRequest(r *http.Request) (RegisterRequest, error) {
	var body schemaBody
	if err := decode(r, &body); err != nil {
		return RegisterRequest{}, err
	}
	return RegisterRequest{Schema: body.Schema, SchemaType: body.SchemaType, References: body.References, ID: body.ID, Version: body.Version}, nil
}

func (s *Server) register(_ http.ResponseWriter, r *http.Request) (any, error) {
	req, err := registerRequest(r)
	if err != nil {
		return nil, err
	}
	id, _, err := s.reg.Register(r.Context(), r.PathValue("subject"), req)
	if err != nil {
		return nil, err
	}
	return map[string]int64{"id": id}, nil
}

func (s *Server) lookup(_ http.ResponseWriter, r *http.Request) (any, error) {
	req, err := registerRequest(r)
	if err != nil {
		return nil, err
	}
	v, err := s.reg.Lookup(r.Context(), r.PathValue("subject"), req, flag(r, "deleted"))
	if err != nil {
		return nil, err
	}
	return versionResponse(v), nil
}

func (s *Server) deleteSubject(_ http.ResponseWriter, r *http.Request) (any, error) {
	return s.reg.DeleteSubject(r.Context(), r.PathValue("subject"), flag(r, "permanent"))
}

func (s *Server) deleteVersion(_ http.ResponseWriter, r *http.Request) (any, error) {
	version, err := pathVersion(r)
	if err != nil {
		return nil, err
	}
	return s.reg.DeleteVersion(r.Context(), r.PathValue("subject"), version, flag(r, "permanent"))
}

func (s *Server) getConfig(_ http.ResponseWriter, r *http.Request) (any, error) {
	level, err := s.reg.Compatibility(r.Context(), r.PathValue("subject"), flag(r, "defaultToGlobal"))
	if err != nil {
		return nil, err
	}
	return map[string]string{"compatibilityLevel": level}, nil
}

func (s *Server) putConfig(_ http.ResponseWriter, r *http.Request) (any, error) {
	var body struct {
		Compatibility string `json:"compatibility"`
	}
	if err := decode(r, &body); err != nil {
		return nil, err
	}
	if err := s.reg.SetCompatibility(r.Context(), r.PathValue("subject"), body.Compatibility); err != nil {
		return nil, err
	}
	return map[string]string{"compatibility": body.Compatibility}, nil
}

func (s *Server) deleteConfig(_ http.ResponseWriter, r *http.Request) (any, error) {
	level, err := s.reg.DeleteCompatibility(r.Context(), r.PathValue("subject"))
	if err != nil {
		return nil, err
	}
	return map[string]string{"compatibilityLevel": level}, nil
}

func (s *Server) getMode(_ http.ResponseWriter, r *http.Request) (any, error) {
	mode, err := s.reg.Mode(r.Context(), r.PathValue("subject"), flag(r, "defaultToGlobal"))
	if err != nil {
		return nil, err
	}
	return map[string]string{"mode": mode}, nil
}

func (s *Server) putMode(_ http.ResponseWriter, r *http.Request) (any, error) {
	var body struct {
		Mode string `json:"mode"`
	}
	if err := decode(r, &body); err != nil {
		return nil, err
	}
	if err := s.reg.SetMode(r.Context(), r.PathValue("subject"), body.Mode, flag(r, "force")); err != nil {
		return nil, err
	}
	return map[string]string{"mode": body.Mode}, nil
}

func (s *Server) deleteMode(_ http.ResponseWriter, r *http.Request) (any, error) {
	mode, err := s.reg.DeleteMode(r.Context(), r.PathValue("subject"))
	if err != nil {
		return nil, err
	}
	return map[string]string{"mode": mode}, nil
}

func (s *Server) compatibility(_ http.ResponseWriter, r *http.Request) (any, error) {
	version, err := pathVersion(r)
	if err != nil {
		return nil, err
	}
	req, err := registerRequest(r)
	if err != nil {
		return nil, err
	}
	ok, messages, err := s.reg.Compatible(r.Context(), r.PathValue("subject"), version, req)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"is_compatible": ok}
	if flag(r, "verbose") {
		if messages == nil {
			messages = []string{}
		}
		out["messages"] = messages
	}
	return out, nil
}
