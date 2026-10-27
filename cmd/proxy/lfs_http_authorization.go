// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/KafScale/platform/pkg/acl"
	"github.com/KafScale/platform/pkg/authn"
	"github.com/twmb/franz-go/pkg/kerr"
)

func (m *lfsModule) authorizeHTTPUpload(w http.ResponseWriter, requestID, topic string) bool {
	if m.authorizer == nil || m.authorizer.Allows(authn.LFSPrincipal, acl.ActionProduce, acl.ResourceTopic, topic) {
		return true
	}
	m.lfsWriteHTTPError(w, requestID, topic, http.StatusForbidden, "topic_authorization_failed", "LFS service cannot produce to this topic")
	return false
}

// A completed TCP exchange is not a Kafka acknowledgement. Require the expected
// partition and correlation ID, and reject every broker error before HTTP 200.
func (m *lfsModule) produceHTTPRecord(ctx context.Context, conn net.Conn, payload []byte, topic string, partition, correlationID int32) (int64, error) {
	data, err := m.forwardToBackend(ctx, conn, payload)
	if err != nil {
		return 0, err
	}
	response, err := parseProduceResponse(data, 9)
	if err != nil {
		return 0, err
	}
	if int32(binary.BigEndian.Uint32(data)) != correlationID {
		return 0, errors.New("produce response correlation mismatch")
	}
	if len(response.Topics) != 1 || response.Topics[0].Topic != topic || len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].Partition != partition {
		return 0, errors.New("produce response does not acknowledge requested partition")
	}
	result := response.Topics[0].Partitions[0]
	if result.ErrorCode != 0 {
		return 0, fmt.Errorf("Kafka produce rejected: %w", kerr.ErrorForCode(result.ErrorCode))
	}
	return result.BaseOffset, nil
}

func produceHTTPStatus(err error) int {
	if errors.Is(err, kerr.TopicAuthorizationFailed) || errors.Is(err, kerr.ClusterAuthorizationFailed) {
		return http.StatusForbidden
	}
	return http.StatusBadGateway
}
