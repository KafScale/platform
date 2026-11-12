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

package operator

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/KafScale/platform/pkg/storage"
)

func TestSnapshotContainersAcceptEitherS3KeyPair(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is required to run the snapshot scripts")
	}
	cases := []struct {
		name                           string
		data                           map[string]string
		wantAccess, wantSecret, wantTk string
	}{
		{name: "aws keys", data: map[string]string{"AWS_ACCESS_KEY_ID": "aws-ak", "AWS_SECRET_ACCESS_KEY": "aws-sk", "AWS_SESSION_TOKEN": "aws-tok"}, wantAccess: "aws-ak", wantSecret: "aws-sk", wantTk: "aws-tok"},
		{name: "kafscale keys", data: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SECRET_KEY": "ks-sk", "KAFSCALE_S3_SESSION_TOKEN": "ks-tok"}, wantAccess: "ks-ak", wantSecret: "ks-sk", wantTk: "ks-tok"},
		{name: "both pairs", data: map[string]string{"AWS_ACCESS_KEY_ID": "aws-ak", "AWS_SECRET_ACCESS_KEY": "aws-sk", "KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SECRET_KEY": "ks-sk"}, wantAccess: "ks-ak", wantSecret: "ks-sk"},
		{name: "empty kafscale session token", data: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SECRET_KEY": "ks-sk", "KAFSCALE_S3_SESSION_TOKEN": "", "AWS_SESSION_TOKEN": "aws-tok"}, wantAccess: "ks-ak", wantSecret: "ks-sk"},
		{name: "whitespace kafscale session token", data: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SECRET_KEY": "ks-sk", "KAFSCALE_S3_SESSION_TOKEN": " \t", "AWS_SESSION_TOKEN": "aws-tok"}, wantAccess: "ks-ak", wantSecret: "ks-sk"},
		{name: "kafscale pair with only an aws session token", data: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SECRET_KEY": "ks-sk", "AWS_SESSION_TOKEN": "aws-tok"}, wantAccess: "ks-ak", wantSecret: "ks-sk", wantTk: "aws-tok"},
		{name: "padded kafscale values", data: map[string]string{"KAFSCALE_S3_ACCESS_KEY": " ks-ak\n", "KAFSCALE_S3_SECRET_KEY": "\tks-sk ", "KAFSCALE_S3_SESSION_TOKEN": " ks-tok\n"}, wantAccess: "ks-ak", wantSecret: "ks-sk", wantTk: "ks-tok"},
		{name: "padded aws values", data: map[string]string{"AWS_ACCESS_KEY_ID": " aws-ak\n", "AWS_SECRET_ACCESS_KEY": "aws-sk ", "AWS_SESSION_TOKEN": "\taws-tok"}, wantAccess: "aws-ak", wantSecret: "aws-sk", wantTk: "aws-tok"},
		{name: "whitespace kafscale keys next to an aws pair", data: map[string]string{"AWS_ACCESS_KEY_ID": "aws-ak", "AWS_SECRET_ACCESS_KEY": "aws-sk", "AWS_SESSION_TOKEN": "aws-tok", "KAFSCALE_S3_ACCESS_KEY": "  ", "KAFSCALE_S3_SECRET_KEY": "\n", "KAFSCALE_S3_SESSION_TOKEN": " "}, wantAccess: "aws-ak", wantSecret: "aws-sk", wantTk: "aws-tok"},
		{name: "empty kafscale keys next to an aws pair", data: map[string]string{"AWS_ACCESS_KEY_ID": "aws-ak", "AWS_SECRET_ACCESS_KEY": "aws-sk", "KAFSCALE_S3_ACCESS_KEY": "", "KAFSCALE_S3_SECRET_KEY": ""}, wantAccess: "aws-ak", wantSecret: "aws-sk"},
		{name: "partial kafscale pair next to an aws pair", data: map[string]string{"AWS_ACCESS_KEY_ID": "aws-ak", "AWS_SECRET_ACCESS_KEY": "aws-sk", "AWS_SESSION_TOKEN": "aws-tok", "KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SESSION_TOKEN": "ks-tok"}, wantAccess: "aws-ak", wantSecret: "aws-sk", wantTk: "aws-tok"},
		{name: "whitespace aws keys next to a kafscale pair", data: map[string]string{"AWS_ACCESS_KEY_ID": " ", "AWS_SECRET_ACCESS_KEY": " ", "AWS_SESSION_TOKEN": " ", "KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SECRET_KEY": "ks-sk"}, wantAccess: "ks-ak", wantSecret: "ks-sk"},
		{name: "only whitespace keys", data: map[string]string{"AWS_ACCESS_KEY_ID": " ", "AWS_SECRET_ACCESS_KEY": " ", "KAFSCALE_S3_ACCESS_KEY": " ", "KAFSCALE_S3_SECRET_KEY": " "}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := testCluster("snap-creds", nil)
			cluster.Spec.S3.CredentialsSecretRef = "s3-creds"
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "default"}, Data: map[string][]byte{}}
			for k, v := range tc.data {
				secret.Data[k] = []byte(v)
			}
			scheme := testScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, secret).Build()
			if _, err := EnsureEtcd(context.Background(), c, scheme, cluster); err != nil {
				t.Fatalf("EnsureEtcd: %v", err)
			}
			sts := &appsv1.StatefulSet{}
			assertFound(t, c, sts, cluster.Namespace, cluster.Name+"-etcd")
			cron := &batchv1.CronJob{}
			assertFound(t, c, cron, cluster.Namespace, cluster.Name+"-etcd-snapshot")

			containers := map[string]corev1.Container{"upload": cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0]}
			for _, ic := range sts.Spec.Template.Spec.InitContainers {
				if ic.Name == "snapshot-download" {
					containers["snapshot-download"] = ic
				}
			}
			if len(containers) != 2 {
				t.Fatalf("snapshot-download init container missing")
			}
			cfg := &storage.S3Config{}
			r := &ClusterReconciler{Client: c, Scheme: scheme}
			if err := r.loadS3Credentials(context.Background(), cluster, cfg); err != nil {
				t.Fatalf("loadS3Credentials: %v", err)
			}
			if cfg.AccessKeyID != tc.wantAccess || cfg.SecretAccessKey != tc.wantSecret || cfg.SessionToken != tc.wantTk {
				t.Errorf("operator snapshot check = %q/%q/%q, want %q/%q/%q", cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken, tc.wantAccess, tc.wantSecret, tc.wantTk)
			}
			want := "access=[" + tc.wantAccess + "] secret=[" + tc.wantSecret + "] token=[" + tc.wantTk + "]"
			for _, name := range []string{"snapshot-download", "upload"} {
				if got := runSnapshotContainerAWS(t, bash, containers[name], secret); got != want {
					t.Errorf("%s aws credentials = %s, want %s", name, got, want)
				}
			}
		})
	}
}

func resolveContainerEnvWithSecret(t *testing.T, container corev1.Container, secret *corev1.Secret) map[string]string {
	t.Helper()
	data := map[string]string{}
	for k, v := range secret.Data {
		data[k] = string(v)
	}
	env := map[string]string{}
	for _, from := range container.EnvFrom {
		if from.SecretRef != nil && from.SecretRef.Name == secret.Name {
			for k, v := range data {
				env[from.Prefix+k] = v
			}
		}
	}
	for k, v := range resolveContainerEnv(t, container.Env, data) {
		env[k] = v
	}
	return env
}

func runSnapshotContainerAWS(t *testing.T, bash string, container corev1.Container, secret *corev1.Secret) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(dir, "aws.record")
	fakeAWS := "#!/bin/sh\n" +
		"printf 'access=[%s] secret=[%s] token=[%s]\\n' \"${AWS_ACCESS_KEY_ID-unset}\" \"${AWS_SECRET_ACCESS_KEY-unset}\" \"${AWS_SESSION_TOKEN-unset}\" | sed 's/\\[unset\\]/[]/g' >> \"$KAFSCALE_TEST_AWS_RECORD\"\n" +
		"case \"$*\" in *list-objects-v2*) printf '\"snap.db\"' ;; esac\n"
	fakePython := "#!/bin/sh\ncat >/dev/null\nprintf 'snap.db'\n"
	for name, body := range map[string]string{"aws": fakeAWS, "python": fakePython} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	snapshots := filepath.Join(dir, "snapshots")
	if err := os.Mkdir(snapshots, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshots, "etcd-snapshot.db"), []byte("snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}
	if len(container.Command) != 3 {
		t.Fatalf("unexpected command for %s: %v", container.Name, container.Command)
	}
	script := strings.ReplaceAll(container.Command[2], "/snapshots/", snapshots+"/")
	script = strings.ReplaceAll(script, "/var/lib/etcd", filepath.Join(dir, "data"))
	env := []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "KAFSCALE_TEST_AWS_RECORD=" + record}
	for k, v := range resolveContainerEnvWithSecret(t, container, secret) {
		if k == "PATH" {
			continue
		}
		if k == "HOME" {
			v = dir
		}
		env = append(env, k+"="+v)
	}
	cmd := exec.Command(bash, "-c", script)
	cmd.Env = env
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s script failed: %v\n%s", container.Name, err, out)
	}
	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("%s did not call aws: %v", container.Name, err)
	}
	return strings.SplitN(strings.TrimSpace(string(data)), "\n", 2)[0]
}
