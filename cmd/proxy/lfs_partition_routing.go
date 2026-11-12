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
	"net"

	"github.com/twmb/franz-go/pkg/kerr"
)

// lfsProduceAttempts bounds how many brokers an LFS HTTP produce is sent to
// when brokers answer that they do not lead the partition.
const lfsProduceAttempts = 3

// errLFSProduceNotWritten marks a produce that no broker wrote, so the
// uploaded object can be deleted instead of kept as an orphan.
var errLFSProduceNotWritten = errors.New("no broker accepted the produce")

// lfsNotPartitionOwner reports whether a broker rejected a produce because it
// does not lead the partition. The record was not written.
func lfsNotPartitionOwner(err error) bool {
	return errors.Is(err, kerr.NotLeaderForPartition) ||
		errors.Is(err, kerr.LeaderNotAvailable) ||
		errors.Is(err, kerr.FencedLeaderEpoch) ||
		errors.Is(err, kerr.UnknownLeaderEpoch)
}

// ownerAddr returns the partition owner from the etcd routes. Static backends
// have no routes.
func (m *lfsModule) ownerAddr(ctx context.Context, topic string, partition int32) string {
	if topic == "" || len(m.backends) > 0 || m.partitionOwner == nil {
		return ""
	}
	return m.partitionOwner(ctx, topic, partition)
}

// partitionCandidates returns the brokers to try for topic/partition: the
// owner when it is known and not tried yet, otherwise every listed broker not
// tried yet.
func (m *lfsModule) partitionCandidates(ctx context.Context, backends []string, topic string, partition int32, tried map[string]bool) []string {
	if owner := m.ownerAddr(ctx, topic, partition); owner != "" && !tried[owner] {
		return []string{owner}
	}
	if len(tried) == 0 {
		return backends
	}
	candidates := make([]string, 0, len(backends))
	for _, addr := range backends {
		if !tried[addr] {
			candidates = append(candidates, addr)
		}
	}
	return candidates
}

// produceToPartitionOwner sends the produce and, when a broker answers that it
// does not lead the partition, sends it again to the current owner, at most
// lfsProduceAttempts times. It returns the connection it ended on, or nil when
// it closed it. Errors wrapping errLFSProduceNotWritten mean no broker wrote
// the record.
func (m *lfsModule) produceToPartitionOwner(ctx context.Context, conn net.Conn, addr string, payload []byte, topic string, partition, correlationID int32) (int64, net.Conn, string, error) {
	tried := make(map[string]bool, lfsProduceAttempts)
	for attempt := 1; ; attempt++ {
		offset, err := m.produceHTTPRecord(ctx, conn, payload, topic, partition, correlationID)
		if err == nil || !lfsNotPartitionOwner(err) {
			return offset, conn, addr, err
		}
		_ = conn.Close()
		tried[addr] = true
		if m.forgetPartitionOwner != nil && m.ownerAddr(ctx, topic, partition) == addr {
			m.forgetPartitionOwner(topic, partition)
		}
		if attempt >= lfsProduceAttempts {
			return 0, nil, "", errors.Join(errLFSProduceNotWritten, err)
		}
		if waitErr := waitBackendRetry(ctx, m.backendBackoff); waitErr != nil {
			return 0, nil, "", errors.Join(errLFSProduceNotWritten, err, waitErr)
		}
		if freshErr := m.checkBackendsFresh(ctx); freshErr != nil {
			return 0, nil, "", errors.Join(errLFSProduceNotWritten, err, freshErr)
		}
		next, nextAddr, connectErr := m.connectPartitionBackend(ctx, topic, partition, tried)
		if connectErr != nil {
			return 0, nil, "", errors.Join(errLFSProduceNotWritten, err, connectErr)
		}
		conn, addr = next, nextAddr
	}
}
