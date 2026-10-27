// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Runs the actual proxy and broker binaries with a normal Kafka client. The
// broker uses embedded etcd and memory S3 so no external infrastructure is needed.
func TestProxyMTLSE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E_AUTH") != "1" {
		t.Skip("set KAFSCALE_E2E_AUTH=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	etcd, endpoints := startEmbeddedEtcd(t)
	t.Cleanup(etcd.Close)
	pki := newTestPKI(t)
	keyFile := filepath.Join(t.TempDir(), "identity")
	writeTestFile(t, keyFile, bytes.Repeat([]byte{83}, 32))
	start := func(component string, env []string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), component+".log")
		log, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", component))
		configureProcessGroup(command)
		command.Env = os.Environ()
		for _, entry := range env {
			key, value, _ := strings.Cut(entry, "=")
			command.Env = upsertEnv(command.Env, key, value)
		}
		command.Stdout = log
		command.Stderr = log
		if err := command.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			stopBroker(t, command)
			log.Close()
			if t.Failed() {
				data, _ := os.ReadFile(path)
				t.Logf("%s logs:\n%s", component, data)
			}
		})
	}
	waitListening := func(addr string) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
			conn, err := net.DialTimeout("tcp", addr, time.Millisecond*100)
			if err == nil {
				conn.Close()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("no listener at %s", addr)
	}
	brokerAddr, proxyAddr := freeAddr(t), freeAddr(t)
	_, brokerPort, _ := net.SplitHostPort(brokerAddr)
	_, proxyPort, _ := net.SplitHostPort(proxyAddr)
	acl := `{"default_policy":"deny","principals":[{"name":"service-a","allow":[{"action":"produce","resource":"topic","name":"orders"},{"action":"fetch","resource":"topic","name":"orders"},{"action":"group_write","resource":"group","name":"group-a"},{"action":"group_read","resource":"group","name":"group-a"}]}]}`
	start("broker", []string{"KAFSCALE_USE_MEMORY_S3=1", "KAFSCALE_AUTO_CREATE_TOPICS=true", "KAFSCALE_AUTO_CREATE_PARTITIONS=1", "KAFSCALE_ACL_ENABLED=true", "KAFSCALE_ACL_FAIL_OPEN=false", "KAFSCALE_ACL_JSON=" + acl, "KAFSCALE_PROXY_IDENTITY_KEY_FILE=" + keyFile, "KAFSCALE_BROKER_ADDR=" + brokerAddr, "KAFSCALE_BROKER_HOST=127.0.0.1", "KAFSCALE_BROKER_PORT=" + brokerPort, "KAFSCALE_METRICS_ADDR=" + freeAddr(t), "KAFSCALE_CONTROL_ADDR=" + freeAddr(t), "KAFSCALE_ETCD_ENDPOINTS=" + strings.Join(endpoints, ",")})
	waitListening(brokerAddr)
	start("proxy", []string{"KAFSCALE_PROXY_ADDR=" + proxyAddr, "KAFSCALE_PROXY_BACKENDS=" + brokerAddr, "KAFSCALE_PROXY_ADVERTISED_HOST=127.0.0.1", "KAFSCALE_PROXY_ADVERTISED_PORT=" + proxyPort, "KAFSCALE_PROXY_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","), "KAFSCALE_PROXY_HEALTH_ADDR=", "KAFSCALE_PROXY_TLS_CERT_FILE=" + pki.options.CertFile, "KAFSCALE_PROXY_TLS_KEY_FILE=" + pki.options.KeyFile, "KAFSCALE_PROXY_TLS_CLIENT_CA_FILE=" + pki.options.ClientCAFile, "KAFSCALE_PROXY_TLS_PRINCIPALS_FILE=" + pki.options.PrincipalsFile, "KAFSCALE_PROXY_IDENTITY_KEY_FILE=" + keyFile})
	waitListening(proxyAddr)
	makeClient := func(identity, clientID string, consume bool) *kgo.Client {
		t.Helper()
		cert, _, _ := pki.certificate(t, "spiffe://test/"+identity, false, false)
		options := []kgo.Opt{kgo.SeedBrokers(proxyAddr), kgo.ClientID(clientID), kgo.AllowAutoTopicCreation(), kgo.DisableIdempotentWrite(), kgo.DialTLSConfig(&tls.Config{RootCAs: pki.roots, ServerName: "localhost", Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}), kgo.RequestTimeoutOverhead(3 * time.Second)}
		if consume {
			options = append(options, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"orders": {0: kgo.NewOffset().AtStart()}}))
		}
		c, err := kgo.NewClient(options...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		return c
	}
	allowed := makeClient("a", "service-b", false)
	denied := makeClient("b", "service-a", false)
	requestCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	if err := allowed.ProduceSync(requestCtx, &kgo.Record{Topic: "orders", Value: []byte("authenticated")}).FirstErr(); err != nil {
		t.Fatal("allowed produce", err)
	}
	if err := denied.ProduceSync(requestCtx, &kgo.Record{Topic: "orders", Value: []byte("spoofed")}).FirstErr(); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("spoofed produce: %v", err)
	}
	consumer := makeClient("a", "service-b", true)
	fetches := consumer.PollFetches(requestCtx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatal(errs)
	}
	records := fetches.Records()
	if len(records) != 1 || string(records[0].Value) != "authenticated" {
		t.Fatal("unexpected records", records)
	}
	join := kmsg.NewPtrJoinGroupRequest()
	join.Group = "group-a"
	group, err := join.RequestWith(requestCtx, denied)
	if err != nil {
		t.Fatal(err)
	}
	if group.ErrorCode != kerr.GroupAuthorizationFailed.Code {
		t.Fatal("group ACL bypass", group.ErrorCode)
	}
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: "admin-only", NumPartitions: 1, ReplicationFactor: 1}}
	admin, err := create.RequestWith(requestCtx, denied)
	if err != nil {
		t.Fatal(err)
	}
	if len(admin.Topics) != 1 || admin.Topics[0].ErrorCode != kerr.TopicAuthorizationFailed.Code {
		t.Fatal("admin ACL bypass", admin)
	}
	metadata := kmsg.NewPtrMetadataRequest()
	listed, err := metadata.RequestWith(requestCtx, denied)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Topics) != 0 {
		t.Fatal("unauthorized metadata exposed", listed.Topics)
	}
}
