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
	"errors"
	"log/slog"
	"net"
	"os"

	"github.com/KafScale/platform/pkg/acl"
	"github.com/KafScale/platform/pkg/authn"
	"github.com/KafScale/platform/pkg/broker"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func loadBrokerConnContext(logger *slog.Logger) (broker.ConnContextFunc, error) {
	keyFile := os.Getenv("KAFSCALE_PROXY_IDENTITY_KEY_FILE")
	if keyFile == "" {
		return buildConnContextFunc(logger), nil
	}
	if !parseEnvBool("KAFSCALE_ACL_ENABLED", false) || parseEnvBool("KAFSCALE_ACL_FAIL_OPEN", false) {
		return nil, errors.New("proxy identity requires ACLs enabled and fail-open disabled")
	}
	if _, err := authn.ReadKey(keyFile); err != nil {
		return nil, err
	}
	return func(conn net.Conn) (net.Conn, *broker.ConnContext, error) {
		key, err := authn.ReadKey(keyFile)
		if err != nil {
			return nil, nil, err
		}
		principal, err := authn.ReceiveIdentity(conn, key)
		if err != nil {
			return nil, nil, err
		}
		return conn, &broker.ConnContext{Principal: principal, RemoteAddr: conn.RemoteAddr().String()}, nil
	}, nil
}

// authorizedMetadata copies the slice so filtering does not mutate store caches.
func (h *handler) authorizedMetadata(principal string, topics []kmsg.MetadataResponseTopic, listAll, byID bool) []kmsg.MetadataResponseTopic {
	result := make([]kmsg.MetadataResponseTopic, 0, len(topics))
	for _, topic := range topics {
		if topic.Topic != nil && !h.allowTopic(principal, *topic.Topic, acl.ActionFetch) && !h.allowTopic(principal, *topic.Topic, acl.ActionProduce) && !h.allowAdmin(principal) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionFetch, acl.ResourceTopic, *topic.Topic)
			if listAll {
				continue
			}
			topic.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
			topic.Partitions = nil
			if byID {
				topic.ErrorCode = protocol.UNKNOWN_TOPIC_ID
				topic.Topic = nil
			} else {
				topic.TopicID = [16]byte{}
			}
			topic.IsInternal = false
			topic.AuthorizedOperations = 0
		}
		result = append(result, topic)
	}
	return result
}
