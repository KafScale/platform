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

package protocol

import "github.com/twmb/franz-go/pkg/kmsg"

// UnsupportedTransactionResponse returns a schema-correct failure for features
// outside nontransactional idempotence. Both proxy and broker use this boundary.
// Per-resource APIs must echo every requested resource with an error; an empty
// response or a zero-initialized response can be interpreted as success.
func UnsupportedTransactionResponse(req kmsg.Request) kmsg.Response {
	const code = UNSUPPORTED_VERSION
	switch r := req.(type) {
	case *kmsg.InitProducerIDRequest:
		if r.TransactionalID != nil {
			resp := kmsg.NewPtrInitProducerIDResponse()
			resp.ErrorCode, resp.ProducerID, resp.ProducerEpoch = code, -1, -1
			return resp
		}
	case *kmsg.FindCoordinatorRequest:
		if r.CoordinatorType != 0 {
			resp := kmsg.NewPtrFindCoordinatorResponse()
			resp.ErrorCode, resp.NodeID, resp.Port = code, -1, -1
			resp.ErrorMessage = kmsg.StringPtr("transaction coordinators are not supported")
			for _, key := range r.CoordinatorKeys {
				resp.Coordinators = append(resp.Coordinators, kmsg.FindCoordinatorResponseCoordinator{
					Key: key, ErrorCode: code, ErrorMessage: resp.ErrorMessage, NodeID: -1, Port: -1,
				})
			}
			return resp
		}
	case *kmsg.ProduceRequest:
		if r.TransactionID != nil {
			return ProduceErrorResponse(r, code)
		}
	case *kmsg.FetchRequest:
		if r.IsolationLevel != 0 {
			resp := kmsg.NewPtrFetchResponse()
			resp.ErrorCode = code
			for _, t := range r.Topics {
				topic := kmsg.NewFetchResponseTopic()
				topic.Topic, topic.TopicID = t.Topic, t.TopicID
				for _, p := range t.Partitions {
					part := kmsg.NewFetchResponseTopicPartition()
					part.Partition, part.ErrorCode = p.Partition, code
					topic.Partitions = append(topic.Partitions, part)
				}
				resp.Topics = append(resp.Topics, topic)
			}
			return resp
		}
	case *kmsg.ListOffsetsRequest:
		if r.IsolationLevel != 0 {
			resp := kmsg.NewPtrListOffsetsResponse()
			for _, t := range r.Topics {
				topic := kmsg.NewListOffsetsResponseTopic()
				topic.Topic = t.Topic
				for _, p := range t.Partitions {
					part := kmsg.NewListOffsetsResponseTopicPartition()
					part.Partition, part.ErrorCode = p.Partition, code
					topic.Partitions = append(topic.Partitions, part)
				}
				resp.Topics = append(resp.Topics, topic)
			}
			return resp
		}
	case *kmsg.AddPartitionsToTxnRequest:
		resp := kmsg.NewPtrAddPartitionsToTxnResponse()
		resp.ErrorCode = code // v4+ has a top-level error
		for _, t := range r.Topics {
			topic := kmsg.NewAddPartitionsToTxnResponseTopic()
			topic.Topic = t.Topic
			for _, p := range t.Partitions {
				topic.Partitions = append(topic.Partitions, kmsg.AddPartitionsToTxnResponseTopicPartition{Partition: p, ErrorCode: code})
			}
			resp.Topics = append(resp.Topics, topic)
		}
		for _, tx := range r.Transactions {
			transaction := kmsg.NewAddPartitionsToTxnResponseTransaction()
			transaction.TransactionalID = tx.TransactionalID
			for _, t := range tx.Topics {
				topic := kmsg.NewAddPartitionsToTxnResponseTransactionTopic()
				topic.Topic = t.Topic
				for _, p := range t.Partitions {
					topic.Partitions = append(topic.Partitions, kmsg.AddPartitionsToTxnResponseTransactionTopicPartition{Partition: p, ErrorCode: code})
				}
				transaction.Topics = append(transaction.Topics, topic)
			}
			resp.Transactions = append(resp.Transactions, transaction)
		}
		return resp
	case *kmsg.AddOffsetsToTxnRequest:
		resp := kmsg.NewPtrAddOffsetsToTxnResponse()
		resp.ErrorCode = code
		return resp
	case *kmsg.EndTxnRequest:
		resp := kmsg.NewPtrEndTxnResponse()
		resp.ErrorCode = code
		return resp
	case *kmsg.WriteTxnMarkersRequest:
		resp := kmsg.NewPtrWriteTxnMarkersResponse()
		for _, m := range r.Markers {
			marker := kmsg.NewWriteTxnMarkersResponseMarker()
			marker.ProducerID = m.ProducerID
			for _, t := range m.Topics {
				topic := kmsg.NewWriteTxnMarkersResponseMarkerTopic()
				topic.Topic = t.Topic
				for _, p := range t.Partitions {
					topic.Partitions = append(topic.Partitions, kmsg.WriteTxnMarkersResponseMarkerTopicPartition{Partition: p, ErrorCode: code})
				}
				marker.Topics = append(marker.Topics, topic)
			}
			resp.Markers = append(resp.Markers, marker)
		}
		return resp
	case *kmsg.TxnOffsetCommitRequest:
		resp := kmsg.NewPtrTxnOffsetCommitResponse()
		for _, t := range r.Topics {
			topic := kmsg.NewTxnOffsetCommitResponseTopic()
			topic.Topic = t.Topic
			for _, p := range t.Partitions {
				topic.Partitions = append(topic.Partitions, kmsg.TxnOffsetCommitResponseTopicPartition{Partition: p.Partition, ErrorCode: code})
			}
			resp.Topics = append(resp.Topics, topic)
		}
		return resp
	case *kmsg.DescribeTransactionsRequest:
		resp := kmsg.NewPtrDescribeTransactionsResponse()
		for _, id := range r.TransactionalIDs {
			resp.TransactionStates = append(resp.TransactionStates, kmsg.DescribeTransactionsResponseTransactionState{
				TransactionalID: id, ErrorCode: code, ProducerID: -1, ProducerEpoch: -1, StartTimestamp: -1,
			})
		}
		return resp
	case *kmsg.ListTransactionsRequest:
		resp := kmsg.NewPtrListTransactionsResponse()
		resp.ErrorCode = code
		return resp
	case *kmsg.AllocateProducerIDsRequest:
		resp := kmsg.NewPtrAllocateProducerIDsResponse()
		resp.ErrorCode = code
		return resp
	}
	return nil
}

func ProduceErrorResponse(req *kmsg.ProduceRequest, code int16) *kmsg.ProduceResponse {
	resp := kmsg.NewPtrProduceResponse()
	for _, t := range req.Topics {
		topic := kmsg.NewProduceResponseTopic()
		topic.Topic = t.Topic
		for _, p := range t.Partitions {
			part := kmsg.NewProduceResponseTopicPartition()
			part.Partition, part.ErrorCode = p.Partition, code
			topic.Partitions = append(topic.Partitions, part)
		}
		resp.Topics = append(resp.Topics, topic)
	}
	return resp
}
