#!/bin/sh
# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

set -eu

if [ "${USE_LOCAL_PLATFORM:-0}" = "1" ]; then
    exit 0
fi

if [ -z "${PLATFORM_VERSION:-}" ]; then
    echo "Standalone builds require --build-arg PLATFORM_VERSION=<published /v2 version>. v2.0.0 has an invalid module path. To build this checkout, use USE_LOCAL_PLATFORM=1 with the repository root as context." >&2
    exit 1
fi

# Validate the actual published artifact before changing the processor module.
# An explicit @version bypasses the local replace directive.
if ! go mod download "github.com/KafScale/platform/v2@${PLATFORM_VERSION}"; then
    echo "Cannot download github.com/KafScale/platform/v2@${PLATFORM_VERSION}. PLATFORM_VERSION must name a published version whose go.mod declares github.com/KafScale/platform/v2; v2.0.0 is not valid." >&2
    exit 1
fi

go mod edit -dropreplace github.com/KafScale/platform/v2
go mod edit "-require=github.com/KafScale/platform/v2@${PLATFORM_VERSION}"
