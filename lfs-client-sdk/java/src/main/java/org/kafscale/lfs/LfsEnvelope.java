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

package org.kafscale.lfs;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class LfsEnvelope {
    @JsonProperty("kfs_lfs")
    public int version;
    public String bucket;
    public String key;
    public long size;
    public String sha256;
    public String checksum;
    @JsonProperty("checksum_alg")
    public String checksumAlg;
    @JsonProperty("content_type")
    public String contentType;
    @JsonProperty("original_headers")
    public Map<String, String> originalHeaders;
    @JsonProperty("created_at")
    public String createdAt;
    @JsonProperty("proxy_id")
    public String proxyId;
}
