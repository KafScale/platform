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

package test

import (
	"regexp"
	"strings"
	"testing"
)

func TestConsoleLinksUseUIPath(t *testing.T) {
	bare := regexp.MustCompile(`https?://(localhost|127\.0\.0\.1):3080(/?[^/u\w]|/?$)`)
	for _, rel := range []string{"README.md", "docs/local-quickstart.md", "docs/s3-connection.md", "deploy/docker-compose/README.md"} {
		doc := readRepoDoc(t, rel)
		for _, line := range strings.Split(doc, "\n") {
			if bare.MatchString(line) {
				t.Errorf("%s links the console without /ui/: %s", rel, strings.TrimSpace(line))
			}
		}
		if !strings.Contains(doc, ":3080/ui/") {
			t.Errorf("%s does not link the console at /ui/", rel)
		}
	}
}

func TestTopicCreateDocsPassReplicationFactor(t *testing.T) {
	create := regexp.MustCompile(`kafka-topics(\.sh)?\s[^\n]*--create(?:[^\n]*\\\n)*[^\n]*`)
	for _, rel := range []string{"examples/E50_JS-kafscale-demo/KAFSCALE-COMPATIBILITY.md", "docs/protocol.md"} {
		commands := create.FindAllString(readRepoDoc(t, rel), -1)
		if len(commands) == 0 {
			t.Errorf("%s has no kafka-topics --create example", rel)
		}
		for _, command := range commands {
			if !strings.Contains(command, "--replication-factor") {
				t.Errorf("%s: kafka-topics --create without --replication-factor: %s", rel, command)
			}
		}
	}
	if !strings.Contains(readRepoDoc(t, "docs/protocol.md"), "CreateTopicRequest version 4+") {
		t.Error("docs/protocol.md does not explain the CreateTopics version requirement")
	}
}

func TestQuickstartKeepsConsolePasswordOffCommandLine(t *testing.T) {
	doc := readRepoDoc(t, "docs/quickstart.md")
	for _, banned := range []string{"--set console.auth.password", "--set 'console.auth.password", `--set "console.auth.password`} {
		if strings.Contains(doc, banned) {
			t.Errorf("docs/quickstart.md suggests %s", banned)
		}
	}
	if !strings.Contains(doc, "console.auth.existingSecret") {
		t.Error("docs/quickstart.md does not point to console.auth.existingSecret")
	}
}

func TestIdempotenceFlagNamesRelease(t *testing.T) {
	for _, rel := range []string{"README.md", "docs/local-quickstart.md"} {
		doc := readRepoDoc(t, rel)
		if strings.Contains(doc, "releases containing #28") || strings.Contains(doc, "Releases\ncontaining #28") {
			t.Errorf("%s refers to releases containing #28 without naming the version", rel)
		}
		if !strings.Contains(doc, "`v2.1.0`") {
			t.Errorf("%s does not name v2.1.0 as the first release with #28", rel)
		}
	}
}

func TestDevStackUsesIPv4Loopback(t *testing.T) {
	compose := readRepoDoc(t, "deploy/docker-compose/docker-compose.yaml")
	if !regexp.MustCompile(`(?m)^\s*KAFSCALE_PROXY_ADVERTISED_HOST:\s*"?127\.0\.0\.1"?\s*$`).MatchString(compose) {
		t.Fatal("development Compose proxy must advertise 127.0.0.1 so clients never resolve localhost to ::1")
	}
	portMap := regexp.MustCompile(`(?m)^\s+-\s+"(?:127\.0\.0\.1:)?\d+:\d+"`)
	for _, line := range portMap.FindAllString(compose, -1) {
		if !strings.Contains(line, `"127.0.0.1:`) {
			t.Errorf("development Compose publishes %s; host ports must bind 127.0.0.1 only", strings.TrimSpace(line))
		}
	}
	if portMap.FindString(compose) == "" {
		t.Fatal("development Compose publishes no quoted host ports")
	}
	for _, rel := range []string{"README.md", "docs/local-quickstart.md", "deploy/docker-compose/README.md", "deploy/docker-compose/Makefile"} {
		doc := readRepoDoc(t, rel)
		for _, stale := range []string{"localhost:9093", "localhost:9094", "localhost:3080"} {
			if strings.Contains(doc, stale) {
				t.Errorf("%s uses %s; use 127.0.0.1 to match the published ports", rel, stale)
			}
		}
		if rel == "deploy/docker-compose/Makefile" {
			continue
		}
		for _, want := range []string{"127.0.0.1:9093", "http://127.0.0.1:3080/ui/"} {
			if !strings.Contains(doc, want) {
				t.Errorf("%s does not mention %s", rel, want)
			}
		}
	}
}
