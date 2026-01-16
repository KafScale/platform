// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

const (
	operatorEtcdSilenceLogsEnv = "KAFSCALE_OPERATOR_ETCD_SILENCE_LOGS"
)

// SnapshotPublisher translates KafscaleCluster/KafscaleTopic resources into
// the metadata snapshot read by brokers.
type SnapshotPublisher struct {
	Client client.Client
}

// NewSnapshotPublisher creates a publisher backed by the provided controller-runtime client.
func NewSnapshotPublisher(c client.Client) *SnapshotPublisher {
	return &SnapshotPublisher{Client: c}
}

// Publish renders the current metadata for the cluster and puts it into etcd.
func (p *SnapshotPublisher) Publish(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, endpoints []string) error {
	var topicList kafscalev1alpha1.KafscaleTopicList
	if err := p.Client.List(ctx, &topicList, client.InNamespace(cluster.Namespace)); err != nil {
		operatorSnapshotResults.WithLabelValues("error").Inc()
		return err
	}
	topics := make([]kafscalev1alpha1.KafscaleTopic, 0, len(topicList.Items))
	for _, topic := range topicList.Items {
		if topic.Spec.ClusterRef == cluster.Name {
			topics = append(topics, topic)
		}
	}
	meta := BuildClusterMetadata(cluster, topics)
	if err := PublishMetadataSnapshot(ctx, endpoints, meta); err != nil {
		operatorSnapshotResults.WithLabelValues("error").Inc()
		return err
	}
	operatorSnapshotResults.WithLabelValues("success").Inc()
	return nil
}

func mergeExistingSnapshot(ctx context.Context, endpoints []string, next metadata.ClusterMetadata) metadata.ClusterMetadata {
	if len(endpoints) == 0 {
		return next
	}
	existing, err := readSnapshotFromEtcd(ctx, endpoints)
	if err != nil || len(existing.Topics) == 0 {
		return next
	}
	seen := make(map[string]struct{}, len(next.Topics))
	for _, topic := range next.Topics {
		if topic.Name == "" {
			continue
		}
		seen[topic.Name] = struct{}{}
	}
	for _, topic := range existing.Topics {
		if topic.Name == "" || topic.ErrorCode != 0 {
			continue
		}
		if _, ok := seen[topic.Name]; ok {
			continue
		}
		next.Topics = append(next.Topics, topic)
	}
	return next
}

func readSnapshotFromEtcd(ctx context.Context, endpoints []string) (metadata.ClusterMetadata, error) {
	var snap metadata.ClusterMetadata
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	if parseBoolEnv(operatorEtcdSilenceLogsEnv) {
		cfg.Logger = zap.NewNop()
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return snap, err
	}
	defer cli.Close()
	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := cli.Get(getCtx, "/kafscale/metadata/snapshot")
	if err != nil {
		return snap, err
	}
	if len(resp.Kvs) == 0 {
		return snap, nil
	}
	if err := json.Unmarshal(resp.Kvs[0].Value, &snap); err != nil {
		return snap, err
	}
	return snap, nil
}

// BuildClusterMetadata converts CRD state into the metadata snapshot consumed by brokers.
func BuildClusterMetadata(cluster *kafscalev1alpha1.KafscaleCluster, topics []kafscalev1alpha1.KafscaleTopic) metadata.ClusterMetadata {
	replicas := int32(1)
	if cluster.Spec.Brokers.Replicas != nil && *cluster.Spec.Brokers.Replicas > 0 {
		replicas = *cluster.Spec.Brokers.Replicas
	}
	brokers := make([]protocol.MetadataBroker, replicas)
	brokerPort := int32(9092)
	if cluster.Spec.Brokers.AdvertisedPort != nil && *cluster.Spec.Brokers.AdvertisedPort > 0 {
		brokerPort = *cluster.Spec.Brokers.AdvertisedPort
	}
	advertisedHost := strings.TrimSpace(cluster.Spec.Brokers.AdvertisedHost)
	headlessSvc := fmt.Sprintf("%s-broker-headless", cluster.Name)
	for i := int32(0); i < replicas; i++ {
		host := advertisedHost
		if replicas > 1 || host == "" {
			host = fmt.Sprintf("%s-broker-%d.%s.%s.svc.cluster.local", cluster.Name, i, headlessSvc, cluster.Namespace)
		}
		brokers[i] = protocol.MetadataBroker{
			NodeID: i,
			Host:   host,
			Port:   brokerPort,
		}
	}
	metaTopics := make([]protocol.MetadataTopic, 0, len(topics))
	replicaIDs := buildReplicaIDs(replicas)
	clusterID := string(cluster.UID)
	var clusterIDPtr *string
	if clusterID != "" {
		clusterIDPtr = &clusterID
	}
	clusterName := cluster.Name
	var clusterNamePtr *string
	if clusterName != "" {
		clusterNamePtr = &clusterName
	}
	for _, topic := range topics {
		partitions := make([]protocol.MetadataPartition, topic.Spec.Partitions)
		for i := int32(0); i < topic.Spec.Partitions; i++ {
			leader := replicaIDs[0]
			if len(replicaIDs) > 0 {
				leader = replicaIDs[i%int32(len(replicaIDs))]
			}
			partitions[i] = protocol.MetadataPartition{
				PartitionIndex: i,
				LeaderID:       leader,
				ReplicaNodes:   replicaIDs,
				ISRNodes:       replicaIDs,
			}
		}
		metaTopics = append(metaTopics, protocol.MetadataTopic{
			Name:       topic.Name,
			TopicID:    metadata.TopicIDForName(topic.Name),
			IsInternal: false,
			Partitions: partitions,
		})
	}
	return metadata.ClusterMetadata{
		Brokers:      brokers,
		ControllerID: 0,
		Topics:       metaTopics,
		ClusterName:  clusterNamePtr,
		ClusterID:    clusterIDPtr,
	}
}

func buildReplicaIDs(replicaCount int32) []int32 {
	if replicaCount <= 0 {
		return nil
	}
	out := make([]int32, replicaCount)
	for i := int32(0); i < replicaCount; i++ {
		out[i] = i
	}
	return out
}

// PublishMetadataSnapshot writes the provided metadata snapshot into etcd so brokers can consume it.
func PublishMetadataSnapshot(ctx context.Context, endpoints []string, snapshot metadata.ClusterMetadata) error {
	if len(endpoints) == 0 {
		return fmt.Errorf("etcd endpoints required")
	}
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	if parseBoolEnv(operatorEtcdSilenceLogsEnv) {
		cfg.Logger = zap.NewNop()
	}
	const snapshotKey = "/kafscale/metadata/snapshot"
	conflictErr := errors.New("snapshot update conflict")
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		cli, err := clientv3.New(cfg)
		if err != nil {
			lastErr = err
		} else {
			getCtx, cancelGet := context.WithTimeout(ctx, 5*time.Second)
			resp, err := cli.Get(getCtx, snapshotKey)
			cancelGet()
			if err != nil {
				lastErr = err
				_ = cli.Close()
				if !isRetryableEtcdError(lastErr) {
					return lastErr
				}
				if err := sleepWithContext(ctx, 2*time.Second); err != nil {
					return lastErr
				}
				continue
			}
			existing := metadata.ClusterMetadata{}
			if len(resp.Kvs) > 0 {
				if err := json.Unmarshal(resp.Kvs[0].Value, &existing); err == nil {
					snapshot = mergeSnapshots(snapshot, existing)
				}
			}
			payload, err := json.Marshal(snapshot)
			if err != nil {
				_ = cli.Close()
				return err
			}
			putCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			txn := cli.Txn(putCtx)
			if len(resp.Kvs) == 0 {
				txn = txn.If(clientv3.Compare(clientv3.Version(snapshotKey), "=", 0))
			} else {
				txn = txn.If(clientv3.Compare(clientv3.ModRevision(snapshotKey), "=", resp.Kvs[0].ModRevision))
			}
			txn = txn.Then(clientv3.OpPut(snapshotKey, string(payload)))
			txnResp, err := txn.Commit()
			cancel()
			_ = cli.Close()
			if err == nil && txnResp.Succeeded {
				return nil
			}
			if err == nil && !txnResp.Succeeded {
				lastErr = conflictErr
			} else {
				lastErr = err
			}
		}
		if errors.Is(lastErr, conflictErr) {
			if err := sleepWithContext(ctx, 200*time.Millisecond); err != nil {
				return lastErr
			}
			continue
		}
		if !isRetryableEtcdError(lastErr) {
			return lastErr
		}
		if err := sleepWithContext(ctx, 2*time.Second); err != nil {
			return lastErr
		}
	}
	return lastErr
}

func mergeSnapshots(next, existing metadata.ClusterMetadata) metadata.ClusterMetadata {
	if len(existing.Topics) == 0 {
		return next
	}
	seen := make(map[string]struct{}, len(next.Topics))
	for _, topic := range next.Topics {
		if topic.Name == "" {
			continue
		}
		seen[topic.Name] = struct{}{}
	}
	for _, topic := range existing.Topics {
		if topic.Name == "" || topic.ErrorCode != 0 {
			continue
		}
		if _, ok := seen[topic.Name]; ok {
			continue
		}
		next.Topics = append(next.Topics, topic)
	}
	return next
}

func isRetryableEtcdError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "transport: Error while dialing") ||
		strings.Contains(msg, "no such host")
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
