package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\n  namespace: dev\n")
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
