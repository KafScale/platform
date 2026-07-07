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

package operator

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

func (r *ClusterReconciler) pollManagedEtcdHealth(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, resolution EtcdResolution) {
	clusterKey := cluster.Namespace + "/" + cluster.Name
	resetEtcdHealthMetrics(clusterKey)

	if !resolution.Managed {
		return
	}

	endpoints := managedEtcdMemberClientEndpoints(cluster)
	if len(endpoints) == 0 {
		return
	}

	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return
	}
	defer func() { _ = cli.Close() }()

	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var maxDbSize int64
	for i, ep := range endpoints {
		member := fmt.Sprintf("etcd-%d", i)
		status, err := cli.Maintenance.Status(pollCtx, ep)
		if err != nil {
			continue
		}
		if status.DbSize > maxDbSize {
			maxDbSize = status.DbSize
		}
		operatorEtcdDbTotalSizeBytes.WithLabelValues(clusterKey, member).Set(float64(status.DbSize))
		operatorEtcdDbInUseBytes.WithLabelValues(clusterKey, member).Set(float64(status.DbSizeInUse))
	}

	quota := etcdQuotaBackendBytes()
	threshold := etcdMaintenanceSizeThresholdBytes(quota)
	if maxDbSize >= threshold {
		operatorEtcdDbSizeHigh.WithLabelValues(clusterKey).Set(1)
	}

	alarmResp, err := cli.AlarmList(pollCtx)
	if err != nil {
		return
	}
	for _, alarm := range alarmResp.Alarms {
		if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE {
			operatorEtcdNospaceAlarm.WithLabelValues(clusterKey).Set(1)
			return
		}
	}
}

func resetEtcdHealthMetrics(clusterKey string) {
	operatorEtcdDbSizeHigh.WithLabelValues(clusterKey).Set(0)
	operatorEtcdNospaceAlarm.WithLabelValues(clusterKey).Set(0)
	for i := 0; i < int(defaultEtcdReplicas); i++ {
		member := fmt.Sprintf("etcd-%d", i)
		operatorEtcdDbTotalSizeBytes.DeleteLabelValues(clusterKey, member)
		operatorEtcdDbInUseBytes.DeleteLabelValues(clusterKey, member)
	}
}

func etcdMaintenanceSizeThresholdPct() int64 {
	raw := strings.TrimSpace(os.Getenv(operatorEtcdMaintenanceSizeThresholdPctEnv))
	if raw == "" {
		return defaultEtcdMaintenanceSizeThresholdPct
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || parsed <= 0 || parsed > 100 {
		return defaultEtcdMaintenanceSizeThresholdPct
	}
	return parsed
}

func etcdMaintenanceSizeThresholdBytes(quota int64) int64 {
	return quota * etcdMaintenanceSizeThresholdPct() / 100
}
