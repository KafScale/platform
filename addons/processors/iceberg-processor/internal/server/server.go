package server

import (
	"context"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler returns the HTTP handler for metrics and health checks.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// Start launches a basic HTTP server for metrics and health checks.
func Start(ctx context.Context, addr string) {
	srv := &http.Server{Addr: addr, Handler: Handler()}

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()
}
