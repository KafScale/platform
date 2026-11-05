// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package server

import (
	"context"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerationAndMetadataReadersAuthenticate(t *testing.T) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{filepath.Join(cfg.Dir, "etcd.log")}
	address := func() url.URL {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		ln.Close()
		u, err := url.Parse("http://" + addr)
		if err != nil {
			t.Fatal(err)
		}
		return *u
	}
	cfg.ListenClientUrls = []url.URL{address()}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{address()}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(5 * time.Second):
		t.Fatal("etcd did not start")
	}
	endpoint := "http://" + e.Clients[0].Addr().String()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for key, value := range map[string]string{topicGenerationPrefix + "orders": "2", "/kafscale/metadata/snapshot": `{"Topics":[{"Topic":"orders","Partitions":[{"Partition":2}]}]}`} {
		if _, err := cli.Put(ctx, key, value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cli.UserAdd(ctx, "root", "test-password"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.UserGrantRole(ctx, "root", "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.AuthEnable(ctx); err != nil {
		t.Fatal(err)
	}
	appCfg := config.Config{Metadata: config.MetaConfig{Etcd: config.EtcdConfig{Endpoints: []string{endpoint}, Username: "root", Password: "test-password"}}}
	srv := New(appCfg, nil)
	srv.cfg.Metadata.Etcd.Password = "incorrect"
	if _, err := srv.getGenerations(); err == nil {
		t.Fatal("invalid credentials accepted")
	}
	srv.cfg.Metadata.Etcd.Password = appCfg.Metadata.Etcd.Password
	source, err := srv.getGenerations()
	if err != nil {
		t.Fatal(err)
	}
	defer source.(*etcdGenerations).client.Close()
	generations, err := source.Generations(ctx)
	if err != nil || generations["orders"] != 2 {
		t.Fatalf("authenticated generation read: %v %v", generations, err)
	}
	appCfg.Metadata.Snapshot.Key = "/kafscale/metadata/snapshot"
	resolver, err := metadata.NewEtcdResolver(appCfg)
	if err != nil {
		t.Fatal(err)
	}
	parts, err := resolver.Partitions(ctx, "orders")
	if err != nil || len(parts) != 1 || parts[0] != 2 {
		t.Fatalf("live snapshot partitions: %v %v", parts, err)
	}
	topics, err := resolver.Topics(ctx)
	if err != nil || len(topics) != 1 || topics[0] != "orders" {
		t.Fatalf("authenticated metadata read: %v %v", topics, err)
	}
}
