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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public final class LfsCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private LfsCodec() {
    }

    public static boolean isEnvelope(byte[] value) {
        if (value == null || value.length < 15) {
            return false;
        }
        if (value[0] != '{') {
            return false;
        }
        int max = Math.min(50, value.length);
        String prefix = new String(value, 0, max);
        return prefix.contains("\"kfs_lfs\"");
    }

    public static LfsEnvelope decode(byte[] value) throws IOException {
        LfsEnvelope env = MAPPER.readValue(value, LfsEnvelope.class);
        if (env.version == 0 || env.bucket == null || env.key == null || env.sha256 == null) {
            throw new IOException("invalid envelope: missing required fields");
        }
        return env;
    }
}
