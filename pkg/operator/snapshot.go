package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
)

// SnapshotPublisher translates KafscaleCluster/KafscaleTopic resources into
// the metadata snapshot consumed by brokers.
type SnapshotPublisher struct {
	Client client.Client
}

// NewSnapshotPublisher builds a publisher backed by the provided controller-runtime client.
func NewSnapshotPublisher(c client.Client) *SnapshotPublisher {
	return &SnapshotPublisher{Client: c}
}

// Publish renders the current metadata for the supplied cluster and puts it into etcd.
func (p *SnapshotPublisher) Publish(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) error {
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
	if err := PublishMetadataSnapshot(ctx, cluster.Spec.Etcd.Endpoints, meta); err != nil {
		operatorSnapshotResults.WithLabelValues("error").Inc()
		return err
	}
	operatorSnapshotResults.WithLabelValues("success").Inc()
	return nil
}

// BuildClusterMetadata converts CRD state into the metadata snapshot consumed by brokers.
func BuildClusterMetadata(cluster *kafscalev1alpha1.KafscaleCluster, topics []kafscalev1alpha1.KafscaleTopic) metadata.ClusterMetadata {
	replicas := int32(1)
	if cluster.Spec.Brokers.Replicas != nil && *cluster.Spec.Brokers.Replicas > 0 {
		replicas = *cluster.Spec.Brokers.Replicas
	}
	brokers := make([]protocol.MetadataBroker, replicas)
	for i := int32(0); i < replicas; i++ {
		brokers[i] = protocol.MetadataBroker{
			NodeID: i,
			Host:   fmt.Sprintf("%s-broker-%d.%s-broker.%s.svc.cluster.local", cluster.Name, i, cluster.Name, cluster.Namespace),
			Port:   9092,
		}
	}
	metaTopics := make([]protocol.MetadataTopic, 0, len(topics))
	replicaIDs := buildReplicaIDs(replicas)
	clusterID := string(cluster.UID)
	var clusterIDPtr *string
	if clusterID != "" {
		clusterIDPtr = &clusterID
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
			Partitions: partitions,
		})
	}
	return metadata.ClusterMetadata{
		Brokers:      brokers,
		ControllerID: 0,
		Topics:       metaTopics,
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
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer cli.Close()
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	putCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = cli.Put(putCtx, "/kafscale/metadata/snapshot", string(payload))
	return err
}
