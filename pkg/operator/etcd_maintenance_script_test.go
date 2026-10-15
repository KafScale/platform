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
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEtcdMaintenanceShellScriptPOSIX(t *testing.T) {
	type scriptShell struct {
		name, path string
		prefix     []string
	}
	shells := []scriptShell{{name: "sh", path: "/bin/sh"}}
	if dash, err := exec.LookPath("dash"); err == nil {
		shells = append(shells, scriptShell{name: "dash", path: dash})
	}
	if busybox, err := exec.LookPath("busybox"); err == nil {
		shells = append(shells, scriptShell{name: "busybox", path: busybox, prefix: []string{"sh"}})
	}
	for _, shell := range shells {
		t.Run(shell.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			args := append(append([]string{}, shell.prefix...), "-n", "-c", etcdMaintenanceShellScript())
			if output, err := exec.CommandContext(ctx, shell.path, args...).CombinedOutput(); err != nil {
				t.Fatalf("shell syntax: %v\n%s", err, output)
			}
			for _, tc := range []struct {
				name, checkOnly, dbSize, alarm, unavailable, failCommand string
				members                                                  []string
				maintenance                                              bool
			}{
				{name: "check_below_threshold", checkOnly: "1", dbSize: "74"},
				{name: "check_at_threshold", checkOnly: "1", dbSize: "75", maintenance: true},
				{name: "check_nospace", checkOnly: "1", dbSize: "1", alarm: "http://member2:2379", maintenance: true},
				{name: "check_status_unavailable", checkOnly: "1", dbSize: "1", unavailable: "http://member1:2379", maintenance: true},
				{name: "scheduled_below_threshold", checkOnly: "0", dbSize: "1", maintenance: true},
				{name: "defrag_failure", checkOnly: "0", dbSize: "1", failCommand: "--endpoints=http://member2:2379 endpoint defrag", maintenance: true},
				{name: "disarm_failure", checkOnly: "0", dbSize: "1", failCommand: "--endpoints=http://member1:2379 alarm disarm", maintenance: true},
				{name: "single_ipv6_member", checkOnly: "0", dbSize: "1", members: []string{"http://[fd00::1]:2379"}, maintenance: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					dir := t.TempDir()
					// This pathname would expand the IPv6 address if globbing were enabled.
					if err := os.Mkdir(filepath.Join(dir, "http:"), 0755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dir, "http:", "f:2379"), nil, 0600); err != nil {
						t.Fatal(err)
					}
					write := func(name, script string) {
						t.Helper()
						if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0755); err != nil {
							t.Fatal(err)
						}
					}
					write("etcdctl", `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$CALL_LOG"
if [ "$*" = "$FAIL_COMMAND" ]; then exit 17; fi
case "$2 $3" in
 "endpoint status")
  if [ "$1" = "--endpoints=$UNAVAILABLE_ENDPOINT" ]; then exit 1; fi
  printf '[{"Status":{"dbSize":%s}}]\n' "$DB_SIZE"
  ;;
 "alarm list")
  if [ "$1" = "--endpoints=$ALARM_ENDPOINT" ]; then printf 'memberID:1 alarm:NOSPACE\n'; fi
  ;;
 "endpoint defrag"|"alarm disarm") ;;
 *) exit 99 ;;
esac
`)
					write("sleep", "#!/bin/sh\nprintf 'sleep %s\\n' \"$*\" >> \"$CALL_LOG\"\n")
					members := tc.members
					if len(members) == 0 {
						members = []string{"http://member1:2379", "http://member2:2379", "http://member3:2379"}
					}
					callLog := filepath.Join(dir, "calls")
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					args := append(append([]string{}, shell.prefix...), "-c", etcdMaintenanceShellScript())
					cmd := exec.CommandContext(ctx, shell.path, args...)
					cmd.Dir = dir
					cmd.Env = append(os.Environ(),
						"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
						"CALL_LOG="+callLog, "ETCD_MEMBERS="+strings.Join(members, ","),
						"ETCD_QUOTA_BYTES=100", "THRESHOLD_PCT=75", "CHECK_ONLY="+tc.checkOnly,
						"DB_SIZE="+tc.dbSize, "ALARM_ENDPOINT="+tc.alarm,
						"UNAVAILABLE_ENDPOINT="+tc.unavailable, "FAIL_COMMAND="+tc.failCommand,
					)
					output, err := cmd.CombinedOutput()
					if tc.failCommand == "" {
						if err != nil {
							t.Fatalf("script: %v\n%s", err, output)
						}
					} else {
						exit, ok := err.(*exec.ExitError)
						if !ok || exit.ExitCode() != 17 {
							t.Fatalf("expected etcdctl exit 17, got %v\n%s", err, output)
						}
					}
					data, err := os.ReadFile(callLog)
					if err != nil {
						t.Fatalf("etcdctl was not called: %v\n%s", err, output)
					}
					var want []string
					for _, ep := range members {
						want = append(want, fmt.Sprintf("--endpoints=%s endpoint status -w json", ep))
						if ep != tc.unavailable {
							want = append(want, fmt.Sprintf("--endpoints=%s alarm list", ep))
						}
					}
					if tc.maintenance {
					maintenance:
						for _, ep := range members {
							for _, operation := range []string{"endpoint defrag", "alarm disarm", "sleep 2"} {
								call := operation
								if operation != "sleep 2" {
									call = "--endpoints=" + ep + " " + operation
								}
								want = append(want, call)
								if call == tc.failCommand {
									break maintenance
								}
							}
						}
					}
					got := strings.Split(strings.TrimSpace(string(data)), "\n")
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("calls:\n%s\nwant:\n%s\noutput:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"), output)
					}
					if !tc.maintenance && !strings.Contains(string(output), "below threshold, skipping maintenance") {
						t.Fatalf("missing skip message: %s", output)
					}
				})
			}
		})
	}
}
