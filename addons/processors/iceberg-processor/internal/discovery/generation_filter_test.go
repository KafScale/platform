// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package discovery

import (
	"context"
	"fmt"
	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestCurrentSegmentsSkipsRetiredGenerationsWithoutCheckpoints(t *testing.T) {
	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	segments := []SegmentRef{{Topic: "orders", StorageTopic: "orders"}, {Topic: "orders", StorageTopic: "orders@1"}, {Topic: "orders", StorageTopic: "orders@2"}, {Topic: "payments"}}
	l := &etcdLister{client: cli}
	for _, tc := range []struct {
		generation string
		want       string
	}{{"0", "orders"}, {"1", "orders@1"}, {"2", "orders@2"}, {"3", ""}} {
		_, err = cli.Put(ctx, metadata.TopicGenerationKey("orders"), tc.generation)
		if err != nil {
			t.Fatal(err)
		}
		got, err := l.currentSegments(ctx, segments)
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if tc.want != "" {
			want = 2
		}
		if len(got) != want {
			t.Fatalf("generation %s: %+v", tc.generation, got)
		}
		if tc.want != "" && got[0].StorageTopic != tc.want {
			t.Fatal(got)
		}
	}
	_, _ = cli.Put(ctx, metadata.TopicGenerationKey("orders"), "bad")
	if _, err := l.currentSegments(ctx, segments); err == nil {
		t.Fatal("corrupt generation accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.currentSegments(canceled, segments); err == nil {
		t.Fatal("canceled read accepted")
	}
}
func TestS3SegmentsChooseLatestGeneration(t *testing.T) {
	segments := []SegmentRef{{Topic: "orders"}, {Topic: "orders", StorageTopic: "orders@2"}, {Topic: "orders", StorageTopic: "orders@1"}, {Topic: "payments"}}
	got := latestSegments(segments)
	if len(got) != 2 || got[0].StorageTopic != "orders@2" || got[1].Topic != "payments" {
		t.Fatal(got)
	}
}

func TestEtcdDiscoveryExcludesDeletedSegments(t *testing.T) {
	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			http.Error(w, "unavailable", 500)
			return
		}
		if r.URL.Query().Get("list-type") != "" {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`)
			for _, topic := range []string{"orders", "orders@1"} {
				for _, suffix := range []string{"kfs", "index"} {
					fmt.Fprintf(w, "<Contents><Key>%s/0/segment-0.%s</Key><Size>64</Size></Contents>", topic, suffix)
				}
			}
			fmt.Fprint(w, `</ListBucketResult>`)
		} else {
			fmt.Fprint(w, "END!")
		}
	}))
	defer server.Close()
	s3client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1}, func(o *s3.Options) { o.BaseEndpoint = aws.String(server.URL); o.UsePathStyle = true })
	listing := &s3Lister{client: s3client, bucket: "test"}
	l := &etcdLister{client: cli, s3: listing, strict: true}
	if _, err := l.ListCompleted(ctx); err == nil {
		t.Fatal("strict discovery accepted missing snapshot")
	}
	l.strict = false
	if _, err := cli.Put(ctx, metadata.TopicGenerationKey("orders"), "1"); err != nil {
		t.Fatal(err)
	}
	got, err := l.ListCompleted(ctx)
	if err != nil || len(got) != 1 || got[0].StorageTopic != "orders@1" {
		t.Fatalf("generation discovery: %v %v", got, err)
	}
	// A fresh processor has no checkpoint, so the generation marker is the fence.
	_, _ = cli.Put(ctx, metadata.TopicGenerationKey("orders"), "2")
	got, err = l.ListCompleted(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("retired data discovered: %v %v", got, err)
	}
	if got, err := listing.ListCompleted(ctx); err != nil || len(got) != 1 {
		t.Fatalf("s3 latest generation: %v %v", got, err)
	}
	_, _ = cli.Put(ctx, metadataSnapshotKey, "broken")
	l.strict = true
	if _, err := l.ListCompleted(ctx); err == nil {
		t.Fatal("strict discovery accepted corrupt metadata")
	}
	l.strict = false
	if got, err := l.ListCompleted(ctx); err != nil || len(got) != 0 {
		t.Fatalf("fallback lost generation fence: %v %v", got, err)
	}
	fail.Store(true)
	if _, err := l.ListCompleted(ctx); err == nil {
		t.Fatal("S3 discovery failure ignored")
	}
}
