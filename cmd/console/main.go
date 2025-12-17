package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	addr := envOrDefault("KAFSCALE_CONSOLE_HTTP_ADDR", ":8080")
	if err := startConsoleServer(ctx, addr); err != nil {
		log.Fatalf("console server failed: %v", err)
	}

	<-ctx.Done()
	log.Println("kafscale console shutting down")
}

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
