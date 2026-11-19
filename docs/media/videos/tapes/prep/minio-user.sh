#!/usr/bin/env bash
# Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/rc.sh" >/dev/null
if [ "$#" -lt 2 ]; then
  echo "usage: $0 add <user> <secret-file> | remove <user>" >&2
  exit 2
fi
action="$1"
user="$2"
port="${KSV_MINIO_ADMIN_PORT:-19000}"
if ! curl -fs -o /dev/null "http://127.0.0.1:$port/minio/health/live"; then
  ksv_port_forward svc/minio "$port:9000"
fi
MC_HOST_ksv="http://$(cat "$KSV_SECRETS/minio-root-user"):$(cat "$KSV_SECRETS/minio-root-pass")@127.0.0.1:$port"
export MC_HOST_ksv
case "$action" in
  add)
    mc admin user add ksv "$user" < "$3"
    mc admin policy attach ksv readwrite --user "$user" || true
    ;;
  remove)
    mc admin user remove ksv "$user"
    ;;
  *)
    echo "unknown action: $action" >&2
    exit 2
    ;;
esac
