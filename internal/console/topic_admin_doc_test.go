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

package console

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

var docStatusCode = regexp.MustCompile("`([1-5][0-9][0-9])`")

func observedTopicAdminErrorCodes(t *testing.T) []int {
	t.Helper()
	seen := map[int]bool{}
	record := func(status int) {
		if status >= 400 {
			seen[status] = true
		}
	}

	c := newTopicAdminClient(t, metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}},
	}), "team-a")
	for _, req := range []struct{ method, path, body string }{
		{http.MethodPost, "/ui/api/status/topics", `{"name":`},
		{http.MethodPost, "/ui/api/status/topics", `{"name":"orders","partitions":1}`},
		{http.MethodPost, "/ui/api/status/topics", `{"name":"orders","partitions":1}`},
		{http.MethodDelete, "/ui/api/status/topics/missing", ""},
		{http.MethodGet, "/ui/api/status/topics", ""},
		{http.MethodPost, "/ui/api/status/topics/orders", ""},
	} {
		status, _ := c.do(req.method, req.path, req.body)
		record(status)
	}

	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		path := "/ui/api/status/topics"
		if method == http.MethodDelete {
			path += "/orders"
		}
		req, err := http.NewRequest(method, c.url+path, strings.NewReader(`{"name":"orders","partitions":1}`))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := c.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		record(resp.StatusCode)
	}

	failing := newTopicAdminClient(t, failingTopicStore{InMemoryStore: metadata.NewInMemoryStore(metadata.ClusterMetadata{}), err: errors.New("snapshot is corrupt")}, "team-a")
	status, _ := failing.do(http.MethodPost, "/ui/api/status/topics", `{"name":"orders","partitions":1}`)
	record(status)

	noStore := newTopicAdminClient(t, nil, "team-a")
	status, _ = noStore.do(http.MethodPost, "/ui/api/status/topics", `{"name":"orders","partitions":1}`)
	record(status)

	codes := make([]int, 0, len(seen))
	for code := range seen {
		codes = append(codes, code)
	}
	slices.Sort(codes)
	return codes
}

func documentedCodes(text string) []int {
	seen := map[int]bool{}
	for _, match := range docStatusCode.FindAllStringSubmatch(text, -1) {
		code, _ := strconv.Atoi(match[1])
		seen[code] = true
	}
	codes := make([]int, 0, len(seen))
	for code := range seen {
		codes = append(codes, code)
	}
	slices.Sort(codes)
	return codes
}

func readRepoDoc(t *testing.T, parts ...string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(append([]string{"..", ".."}, parts...)...))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestConsoleTopicAdminDocumentedErrorCodesMatchHandlers(t *testing.T) {
	want := observedTopicAdminErrorCodes(t)
	for _, code := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusConflict, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		if !slices.Contains(want, code) {
			t.Fatalf("topic endpoints did not return %d; observed %v", code, want)
		}
	}

	ops := readRepoDoc(t, "docs", "operations.md")
	start := strings.Index(ops, "- `POST /ui/api/status/topics` creates a topic.")
	if start < 0 {
		t.Fatal("docs/operations.md has no console topic endpoint list")
	}
	var errorsLine string
	for _, line := range strings.Split(ops[start:], "\n") {
		if strings.HasPrefix(line, "- Errors:") {
			errorsLine = line
			break
		}
	}
	if errorsLine == "" {
		t.Fatal("docs/operations.md has no Errors line for the console topic endpoints")
	}
	if got := documentedCodes(errorsLine); !slices.Equal(got, want) {
		t.Errorf("docs/operations.md console topic errors list %v, handlers return %v", got, want)
	}

	var note string
	for _, line := range strings.Split(readRepoDoc(t, "docs", "releases", "unreleased.md"), "\n") {
		if strings.Contains(line, "/ui/api/status/topics") && strings.Contains(line, "(#362)") {
			note = line
		}
	}
	if note == "" {
		t.Fatal("docs/releases/unreleased.md has no #362 note for the console topic endpoints")
	}
	idx := strings.Index(note, "errors return ")
	if idx < 0 {
		t.Fatal("docs/releases/unreleased.md #362 note does not list the error codes")
	}
	clause := note[idx:]
	if end := strings.Index(clause, ". "); end >= 0 {
		clause = clause[:end]
	}
	if got := documentedCodes(clause); !slices.Equal(got, want) {
		t.Errorf("docs/releases/unreleased.md #362 errors list %v, handlers return %v", got, want)
	}
}
