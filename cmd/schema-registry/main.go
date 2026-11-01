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

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/KafScale/platform/pkg/schemaregistry"
	"github.com/KafScale/platform/pkg/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type settings struct {
	addr        string
	endpoints   []string
	username    string
	password    string
	timeout     time.Duration
	inlineLimit int
	namespace   string
	s3          *storage.S3Config
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Getenv); err != nil {
		log.Fatal(err)
	}
}

func loadSettings(getenv func(string) string) (settings, error) {
	s := settings{
		addr:        getenv("KAFSCALE_SCHEMA_REGISTRY_ADDR"),
		username:    getenv("KAFSCALE_SCHEMA_REGISTRY_ETCD_USERNAME"),
		password:    getenv("KAFSCALE_SCHEMA_REGISTRY_ETCD_PASSWORD"),
		timeout:     10 * time.Second,
		inlineLimit: schemaregistry.DefaultInlineLimit,
		namespace:   strings.TrimSpace(getenv("KAFSCALE_S3_NAMESPACE")),
	}
	if s.addr == "" {
		s.addr = ":8081"
	}
	if s.namespace == "" {
		s.namespace = "default"
	}
	for _, e := range strings.Split(getenv("KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS"), ",") {
		if e = strings.TrimSpace(e); e != "" {
			s.endpoints = append(s.endpoints, e)
		}
	}
	if len(s.endpoints) == 0 {
		return s, errors.New("KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS is required")
	}
	if raw := getenv("KAFSCALE_SCHEMA_REGISTRY_REQUEST_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return s, fmt.Errorf("invalid KAFSCALE_SCHEMA_REGISTRY_REQUEST_TIMEOUT %q", raw)
		}
		s.timeout = d
	}
	if raw := getenv("KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return s, fmt.Errorf("invalid KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES %q", raw)
		}
		s.inlineLimit = n
	}
	if bucket := getenv("KAFSCALE_S3_BUCKET"); bucket != "" {
		endpoint := getenv("KAFSCALE_S3_ENDPOINT")
		pathStyle := endpoint != ""
		if raw := getenv("KAFSCALE_S3_PATH_STYLE"); raw != "" {
			v, err := strconv.ParseBool(raw)
			if err != nil {
				return s, fmt.Errorf("invalid KAFSCALE_S3_PATH_STYLE %q", raw)
			}
			pathStyle = v
		}
		s.s3 = &storage.S3Config{
			Bucket:          bucket,
			Region:          getenv("KAFSCALE_S3_REGION"),
			Endpoint:        endpoint,
			ForcePathStyle:  pathStyle,
			AccessKeyID:     getenv("KAFSCALE_S3_ACCESS_KEY"),
			SecretAccessKey: getenv("KAFSCALE_S3_SECRET_KEY"),
			SessionToken:    getenv("KAFSCALE_S3_SESSION_TOKEN"),
			KMSKeyARN:       getenv("KAFSCALE_S3_KMS_ARN"),
		}
	}
	return s, nil
}

func namespaceWarning(s settings) string {
	if s.s3 == nil || s.namespace != "default" {
		return ""
	}
	return fmt.Sprintf("KAFSCALE_S3_NAMESPACE is %q; schema id records go to %s/@schemas/ in bucket %s, and registries on different etcd clusters that use this bucket and namespace share one schema id space; set KAFSCALE_S3_NAMESPACE per cluster", s.namespace, s.namespace, s.s3.Bucket)
}

func run(ctx context.Context, getenv func(string) string) error {
	s, err := loadSettings(getenv)
	if err != nil {
		return err
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   s.endpoints,
		Username:    s.username,
		Password:    s.password,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("connect to etcd: %w", err)
	}
	defer cli.Close()
	cfg := schemaregistry.Config{Client: cli, Namespace: s.namespace, InlineLimit: s.inlineLimit}
	if s.s3 != nil {
		if cfg.S3, err = storage.NewS3Client(ctx, *s.s3); err != nil {
			return fmt.Errorf("create S3 client: %w", err)
		}
	} else {
		log.Printf("KAFSCALE_S3_BUCKET not set; schemas above %d bytes are rejected and issued schema ids are recorded only in etcd", s.inlineLimit)
	}
	if msg := namespaceWarning(s); msg != "" {
		log.Print(msg)
	}
	reg, err := schemaregistry.New(cfg)
	if err != nil {
		return err
	}
	probeCtx, probeCancel := context.WithTimeout(ctx, 30*time.Second)
	err = reg.CheckIDMarkers(probeCtx)
	probeCancel()
	if err != nil {
		return err
	}
	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if err := reg.Init(initCtx); err != nil {
		log.Printf("read schema id counter from etcd: %v", err)
	}
	cancel()
	srv := &http.Server{Addr: s.addr, Handler: schemaregistry.NewServer(reg, s.timeout), ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("kafscale schema registry listening on %s", s.addr)
		errCh <- srv.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	return srv.Shutdown(shutdownCtx)
}
