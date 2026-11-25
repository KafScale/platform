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
	"errors"

	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var errIdempotentAcks = errors.New("idempotent produce requires acks=all")

func (h *handler) handleInitProducerID(ctx context.Context, header *protocol.RequestHeader, req *kmsg.InitProducerIDRequest) ([]byte, error) {
	resp := kmsg.NewPtrInitProducerIDResponse()
	resp.ProducerID, resp.ProducerEpoch = -1, -1
	allocator, ok := h.store.(metadata.ProducerIDAllocator)
	switch {
	case header.APIVersion < 0 || header.APIVersion > 4 || req.TransactionalID != nil || !ok:
		resp.ErrorCode = protocol.UNSUPPORTED_VERSION
	case !h.etcdAvailable():
		resp.ErrorCode = protocol.COORDINATOR_NOT_AVAILABLE
	default:
		id, err := allocator.AllocateProducerID(ctx)
		if err != nil {
			resp.ErrorCode = protocol.COORDINATOR_NOT_AVAILABLE
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				resp.ErrorCode = protocol.REQUEST_TIMED_OUT
			}
			h.logger.Warn("producer ID allocation failed", "error", err)
		} else {
			resp.ProducerID, resp.ProducerEpoch = id, 0
		}
	}
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}
