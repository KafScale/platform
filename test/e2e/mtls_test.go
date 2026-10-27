// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/authn"
	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	var objectsMu sync.Mutex
	objects := make(map[string][]byte)
	objectStore := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "HEAD":
			w.WriteHeader(200)
		case "PUT":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read failed", 500)
				return
			}
			objectsMu.Lock()
			objects[r.URL.Path] = data
			objectsMu.Unlock()
			w.Header().Set("ETag", `"fixture-etag"`)
			w.WriteHeader(200)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(objectStore.Close)
	objectCount := func() int { objectsMu.Lock(); defer objectsMu.Unlock(); return len(objects) }
	httpAddr := freeAddr(t)

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
	acl := `{"default_policy":"deny","principals":[{"name":"service-a","allow":[{"action":"produce","resource":"topic","name":"mtls-orders"},{"action":"fetch","resource":"topic","name":"mtls-orders"},{"action":"group_write","resource":"group","name":"group-a"},{"action":"group_read","resource":"group","name":"group-a"}]},{"name":"__kafscale_lfs_service","allow":[{"action":"produce","resource":"topic","name":"mtls-orders"}]}]}`
	start("broker", []string{"KAFSCALE_USE_MEMORY_S3=1", "KAFSCALE_AUTO_CREATE_TOPICS=true", "KAFSCALE_AUTO_CREATE_PARTITIONS=1", "KAFSCALE_ACL_ENABLED=true", "KAFSCALE_ACL_FAIL_OPEN=false", "KAFSCALE_ACL_JSON=" + acl, "KAFSCALE_PROXY_IDENTITY_KEY_FILE=" + keyFile, "KAFSCALE_BROKER_ADDR=" + brokerAddr, "KAFSCALE_BROKER_HOST=127.0.0.1", "KAFSCALE_BROKER_PORT=" + brokerPort, "KAFSCALE_METRICS_ADDR=" + freeAddr(t), "KAFSCALE_CONTROL_ADDR=" + freeAddr(t), "KAFSCALE_ETCD_ENDPOINTS=" + strings.Join(endpoints, ",")})
	waitListening(brokerAddr)
	start("proxy", []string{"KAFSCALE_PROXY_LFS_ENABLED=true", "KAFSCALE_LFS_PROXY_S3_BUCKET=mtls-lfs-data", "KAFSCALE_LFS_PROXY_S3_REGION=us-east-1", "KAFSCALE_LFS_PROXY_S3_ENDPOINT=" + objectStore.URL,
		"KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=fixture-access", "KAFSCALE_LFS_PROXY_S3_SECRET_KEY=fixture-secret", "KAFSCALE_LFS_TRACKER_ENABLED=false", "AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED",
		"KAFSCALE_LFS_PROXY_HTTP_ADDR=" + httpAddr, "KAFSCALE_LFS_PROXY_HTTP_API_KEY=fixture-http-key", "KAFSCALE_ACL_JSON=" + acl,
		"KAFSCALE_PROXY_ADDR=" + proxyAddr, "KAFSCALE_PROXY_BACKENDS=" + brokerAddr, "KAFSCALE_PROXY_ADVERTISED_HOST=127.0.0.1", "KAFSCALE_PROXY_ADVERTISED_PORT=" + proxyPort, "KAFSCALE_PROXY_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","), "KAFSCALE_PROXY_HEALTH_ADDR=", "KAFSCALE_PROXY_TLS_CERT_FILE=" + pki.options.CertFile, "KAFSCALE_PROXY_TLS_KEY_FILE=" + pki.options.KeyFile, "KAFSCALE_PROXY_TLS_CLIENT_CA_FILE=" + pki.options.ClientCAFile, "KAFSCALE_PROXY_TLS_PRINCIPALS_FILE=" + pki.options.PrincipalsFile, "KAFSCALE_PROXY_IDENTITY_KEY_FILE=" + keyFile})
	waitListening(proxyAddr)
	waitListening(httpAddr)
	makeClient := func(identity, clientID string, consume bool) *kgo.Client {
		t.Helper()
		cert, _, _ := pki.certificate(t, "spiffe://test/"+identity, false, false)
		options := []kgo.Opt{kgo.SeedBrokers(proxyAddr), kgo.ClientID(clientID), kgo.DisableIdempotentWrite(), kgo.DialTLSConfig(&tls.Config{RootCAs: pki.roots, ServerName: "localhost", Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}), kgo.RequestTimeoutOverhead(3 * time.Second)}
		if consume {
			options = append(options, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"mtls-orders": {0: kgo.NewOffset().AtStart()}}))
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
	if err := allowed.ProduceSync(requestCtx, &kgo.Record{Topic: "mtls-orders", Value: []byte("authenticated")}).FirstErr(); err != nil {
		t.Fatal("allowed produce", err)
	}
	if err := denied.ProduceSync(requestCtx, &kgo.Record{Topic: "mtls-orders", Value: []byte("spoofed")}).FirstErr(); !errors.Is(err, kerr.TopicAuthorizationFailed) {
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
	metadataRequest := kmsg.NewPtrMetadataRequest()
	listed, err := metadataRequest.RequestWith(requestCtx, denied)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Topics) != 0 {
		t.Fatal("unauthorized metadata exposed", listed.Topics)
	}
	// Exercise the Kafka LFS rewrite and HTTP publish through authenticated backends.
	blob := []byte("LFS over an authenticated connection")
	sum := sha256.Sum256(blob)
	record := &kgo.Record{Topic: "mtls-orders", Value: blob, Headers: []kgo.RecordHeader{{Key: "LFS_BLOB", Value: []byte(hex.EncodeToString(sum[:]))}}}
	if err := allowed.ProduceSync(requestCtx, record).FirstErr(); err != nil {
		t.Fatal("mTLS LFS produce", err)
	}
	before := objectCount()
	if before != 1 {
		t.Fatal("LFS object missing", before)
	}
	if err := denied.ProduceSync(requestCtx, &kgo.Record{Topic: "mtls-orders", Value: blob, Headers: record.Headers}).FirstErr(); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatal("denied LFS produce", err)
	}
	if objectCount() != before {
		t.Fatal("unauthorized Kafka client uploaded an object")
	}
	request, err := http.NewRequestWithContext(requestCtx, "POST", "http://"+httpAddr+"/lfs/produce", bytes.NewReader(blob))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Kafka-Topic", "mtls-orders")
	request.Header.Set("X-API-Key", "fixture-http-key")
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("LFS HTTP acknowledgement %d: %s", response.StatusCode, body)
	}
	if _, err := lfs.DecodeEnvelope(body); err != nil {
		t.Fatal("invalid HTTP envelope", err)
	}
	var envelopes int
	for envelopes < 2 {
		fetched := consumer.PollFetches(requestCtx)
		if errs := fetched.Errors(); len(errs) != 0 {
			t.Fatal(errs)
		}
		for _, record := range fetched.Records() {
			if _, err := lfs.DecodeEnvelope(record.Value); err != nil {
				t.Fatal("LFS Kafka envelope", err)
			}
			envelopes++
		}
	}
	if objectCount() != 2 {
		t.Fatal("unexpected object count", objectCount())
	}

	// Rotate the shared key and server certificate while both processes are live.
	rotatedKey := bytes.Repeat([]byte{97}, 32)
	writeTestFile(t, keyFile, rotatedKey)
	for _, key := range [][]byte{rotatedKey, bytes.Repeat([]byte{83}, 32)} {
		conn, err := net.DialTimeout("tcp", brokerAddr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		err = authn.SendIdentity(conn, key, "service-a")
		conn.Close()
		if bytes.Equal(key, rotatedKey) && err != nil {
			t.Fatal("broker did not reload rotated key", err)
		}
		if !bytes.Equal(key, rotatedKey) && err == nil {
			t.Fatal("broker still accepts retired key")
		}
	}
	pki.rotateServer(t)
	serverConfig, _, err := pki.options.Config()
	if err != nil {
		t.Fatal(err)
	}
	expectedCertificate, err := x509.ParseCertificate(serverConfig.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	clientCertificate, _, _ := pki.certificate(t, "spiffe://test/a", false, false)
	rotatedTLS, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", proxyAddr, &tls.Config{RootCAs: pki.roots, ServerName: "localhost", MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{clientCertificate}})
	if err != nil {
		t.Fatal(err)
	}
	actualSerial := rotatedTLS.ConnectionState().PeerCertificates[0].SerialNumber
	rotatedTLS.Close()
	if actualSerial.Cmp(expectedCertificate.SerialNumber) != 0 {
		t.Fatal("proxy did not reload the server certificate")
	}
	renewed := makeClient("a", "still-not-the-principal", false)
	if err := renewed.ProduceSync(requestCtx, &kgo.Record{Topic: "mtls-orders", Value: []byte("after rotation")}).FirstErr(); err != nil {
		t.Fatal("live key/certificate rotation", err)
	}

	// Supply a real topic ID in the operator's metadata snapshot, then compare
	// authorized and unauthorized ID lookups through the proxy.
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer etcdClient.Close()
	snapshot, err := etcdClient.Get(requestCtx, "/kafscale/metadata/snapshot")
	if err != nil || len(snapshot.Kvs) != 1 {
		t.Fatal("snapshot", err)
	}
	var state metadata.ClusterMetadata
	if err := json.Unmarshal(snapshot.Kvs[0].Value, &state); err != nil {
		t.Fatal(err)
	}
	topicID := [16]byte{7}
	for i := range state.Topics {
		if state.Topics[i].Topic != nil && *state.Topics[i].Topic == "mtls-orders" {
			state.Topics[i].TopicID = topicID
			state.Topics[i].IsInternal = true
		}
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := etcdClient.Put(requestCtx, "/kafscale/metadata/snapshot", string(encoded)); err != nil {
		t.Fatal(err)
	}
	byID := kmsg.NewPtrMetadataRequest()
	byID.Topics = []kmsg.MetadataRequestTopic{{TopicID: topicID}}
	for deadline := time.Now().Add(5 * time.Second); ; {
		result, err := byID.RequestWith(requestCtx, renewed)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Topics) == 1 && result.Topics[0].ErrorCode == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("topic ID snapshot not observed", result)
		}
		time.Sleep(20 * time.Millisecond)
	}
	hidden, err := byID.RequestWith(requestCtx, denied)
	if err != nil {
		t.Fatal(err)
	}
	if len(hidden.Topics) != 1 || hidden.Topics[0].ErrorCode != kerr.UnknownTopicID.Code || hidden.Topics[0].Topic != nil || hidden.Topics[0].IsInternal {
		t.Fatal("topic ID disclosed unauthorized metadata", hidden)
	}

	// Removing a mapping rejects new connections without restarting the proxy.
	writeTestFile(t, pki.options.PrincipalsFile, []byte(`{"spiffe://test/b":"service-b"}`))
	cert, _, _ := pki.certificate(t, "spiffe://test/a", false, false)
	connection, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", proxyAddr, &tls.Config{RootCAs: pki.roots, ServerName: "localhost", Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	if err == nil {
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(time.Second))
		req := kmsg.NewPtrApiVersionsRequest()
		wire := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)
		_, _ = connection.Write(wire)
		if _, err := protocol.ReadFrame(connection); err == nil {
			t.Fatal("revoked certificate mapping accepted")
		}
	}

}
