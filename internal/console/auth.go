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

package console

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

const sessionCookieName = "kafscale_ui_session"

type AuthConfig struct {
	Username string
	Password string
}

type authManager struct {
	enabled  bool
	username string
	password string
	ttl      time.Duration
	mu       sync.Mutex
	sessions map[string]time.Time
}

type authConfigResponse struct {
	Enabled bool   `json:"enabled"`
	Message string `json:"message,omitempty"`
}

type authSessionResponse struct {
	Enabled       bool   `json:"enabled"`
	Authenticated bool   `json:"authenticated"`
	Message       string `json:"message,omitempty"`
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Authenticated bool   `json:"authenticated"`
	Message       string `json:"message,omitempty"`
}

func newAuthManager(cfg AuthConfig) *authManager {
	enabled := cfg.Username != "" && cfg.Password != ""
	return &authManager{
		enabled:  enabled,
		username: cfg.Username,
		password: cfg.Password,
		ttl:      12 * time.Hour,
		sessions: make(map[string]time.Time),
	}
}

func (a *authManager) handleConfig(w http.ResponseWriter, _ *http.Request) {
	resp := authConfigResponse{Enabled: a.enabled}
	if !a.enabled {
		resp.Message = "UI access is disabled until KAFSCALE_UI_USERNAME and KAFSCALE_UI_PASSWORD are set."
	}
	writeJSON(w, resp)
}

func (a *authManager) handleSession(w http.ResponseWriter, r *http.Request) {
	resp := authSessionResponse{Enabled: a.enabled}
	if !a.enabled {
		resp.Message = "UI access is disabled until credentials are configured."
		writeJSON(w, resp)
		return
	}
	if a.hasValidSession(r) {
		resp.Authenticated = true
		writeJSON(w, resp)
		return
	}
	writeJSON(w, resp)
}

func (a *authManager) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !a.enabled {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, loginResponse{
			Authenticated: false,
			Message:       "UI access is disabled until credentials are configured.",
		})
		return
	}
	var payload loginRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if !a.validCredentials(payload.Username, payload.Password) {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	token, err := generateToken(32)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}
	expiry := time.Now().Add(a.ttl)
	a.mu.Lock()
	a.sessions[token] = expiry
	a.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil,
		Expires:  expiry,
	})
	writeJSON(w, loginResponse{Authenticated: true})
}

func (a *authManager) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if token, ok := a.sessionToken(r); ok {
		a.mu.Lock()
		delete(a.sessions, token)
		a.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil,
	})
	writeJSON(w, loginResponse{Authenticated: false})
}

func (a *authManager) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.enabled {
			http.Error(w, "ui auth disabled", http.StatusServiceUnavailable)
			return
		}
		if !a.hasValidSession(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (a *authManager) hasValidSession(r *http.Request) bool {
	token, ok := a.sessionToken(r)
	if !ok {
		return false
	}
	a.mu.Lock()
	expiry, exists := a.sessions[token]
	if !exists {
		a.mu.Unlock()
		return false
	}
	if time.Now().After(expiry) {
		delete(a.sessions, token)
		a.mu.Unlock()
		return false
	}
	a.mu.Unlock()
	return true
}

func (a *authManager) sessionToken(r *http.Request) (string, bool) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" {
		return "", false
	}
	return cookie.Value, true
}

func (a *authManager) validCredentials(username, password string) bool {
	if username == "" || password == "" {
		return false
	}
	userOK := subtle.ConstantTimeCompare([]byte(username), []byte(a.username)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(password), []byte(a.password)) == 1
	return userOK && passOK
}

func generateToken(size int) (string, error) {
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
