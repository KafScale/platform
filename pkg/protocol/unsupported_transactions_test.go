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

import (
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestUnsupportedTransactionsRoundTrip(t *testing.T) {
	requests := []kmsg.Request{
		&kmsg.InitProducerIDRequest{TransactionalID: kmsg.StringPtr("tx")},
		&kmsg.FindCoordinatorRequest{CoordinatorType: 1, CoordinatorKey: "tx", CoordinatorKeys: []string{"tx"}},
		&kmsg.ProduceRequest{TransactionID: kmsg.StringPtr("tx"), Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 2}}}}},
		&kmsg.AddPartitionsToTxnRequest{Topics: []kmsg.AddPartitionsToTxnRequestTopic{{Topic: "orders", Partitions: []int32{2}}}, Transactions: []kmsg.AddPartitionsToTxnRequestTransaction{{TransactionalID: "tx", Topics: []kmsg.AddPartitionsToTxnRequestTransactionTopic{{Topic: "orders", Partitions: []int32{2}}}}}},
		kmsg.NewPtrAddOffsetsToTxnRequest(), kmsg.NewPtrEndTxnRequest(),
		&kmsg.WriteTxnMarkersRequest{Markers: []kmsg.WriteTxnMarkersRequestMarker{{ProducerID: 7, Topics: []kmsg.WriteTxnMarkersRequestMarkerTopic{{Topic: "orders", Partitions: []int32{2}}}}}},
		&kmsg.TxnOffsetCommitRequest{Topics: []kmsg.TxnOffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.TxnOffsetCommitRequestTopicPartition{{Partition: 2}}}}},
		&kmsg.DescribeTransactionsRequest{TransactionalIDs: []string{"tx"}},
		kmsg.NewPtrListTransactionsRequest(), kmsg.NewPtrAllocateProducerIDsRequest(),
	}
	for _, req := range requests {
		for version := int16(0); version <= req.MaxVersion(); version++ {
			// The request schemas did not contain these fields in earlier versions.
			if req.Key() == APIKeyFindCoordinator && version == 0 || req.Key() == APIKeyProduce && version < 3 {
				continue
			}
			t.Run(fmt.Sprintf("%s/v%d", kmsg.NameForKey(req.Key()), version), func(t *testing.T) {
				req.SetVersion(version)
				resp := UnsupportedTransactionResponse(req)
				if resp == nil {
					t.Fatal("missing unsupported response")
				}
				wire := EncodeResponse(28, version, resp)
				body, ok := SkipResponseHeader(req.Key(), version, wire)
				if !ok {
					t.Fatal("invalid header")
				}
				decoded := req.ResponseKind()
				decoded.SetVersion(version)
				if err := decoded.ReadFrom(body); err != nil {
					t.Fatal(err)
				}
				var code int16
				switch r := decoded.(type) {
				case *kmsg.InitProducerIDResponse:
					code = r.ErrorCode
					if r.ProducerID != -1 || r.ProducerEpoch != -1 {
						t.Fatal(r)
					}
				case *kmsg.FindCoordinatorResponse:
					if version >= 4 {
						code = r.Coordinators[0].ErrorCode
					} else {
						code = r.ErrorCode
					}
				case *kmsg.ProduceResponse:
					code = r.Topics[0].Partitions[0].ErrorCode
				case *kmsg.AddPartitionsToTxnResponse:
					if version < 4 {
						code = r.Topics[0].Partitions[0].ErrorCode
					} else {
						code = r.Transactions[0].Topics[0].Partitions[0].ErrorCode
						if r.ErrorCode != UNSUPPORTED_VERSION {
							t.Fatal(r)
						}
					}
				case *kmsg.AddOffsetsToTxnResponse:
					code = r.ErrorCode
				case *kmsg.EndTxnResponse:
					code = r.ErrorCode
				case *kmsg.WriteTxnMarkersResponse:
					code = r.Markers[0].Topics[0].Partitions[0].ErrorCode
					if r.Markers[0].ProducerID != 7 {
						t.Fatal(r)
					}
				case *kmsg.TxnOffsetCommitResponse:
					code = r.Topics[0].Partitions[0].ErrorCode
				case *kmsg.DescribeTransactionsResponse:
					code = r.TransactionStates[0].ErrorCode
					if r.TransactionStates[0].TransactionalID != "tx" {
						t.Fatal(r)
					}
				case *kmsg.ListTransactionsResponse:
					code = r.ErrorCode
				case *kmsg.AllocateProducerIDsResponse:
					code = r.ErrorCode
				}
				if code != UNSUPPORTED_VERSION {
					t.Fatalf("error code %d: %+v", code, decoded)
				}
			})
		}
	}
	for _, req := range []kmsg.Request{kmsg.NewPtrInitProducerIDRequest(), &kmsg.FetchRequest{Version: 4, IsolationLevel: 1}, kmsg.NewPtrFetchRequest(), &kmsg.ListOffsetsRequest{Version: 2, IsolationLevel: 1}, kmsg.NewPtrListOffsetsRequest(), kmsg.NewPtrFindCoordinatorRequest(), kmsg.NewPtrProduceRequest()} {
		if resp := UnsupportedTransactionResponse(req); resp != nil {
			t.Fatalf("nontransactional request rejected: %T", req)
		}
	}
}
