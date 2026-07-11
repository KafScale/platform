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

public class LfsResolver {
    private final S3Reader s3;
    private final boolean validateChecksum;
    private final long maxSize;

    public LfsResolver(S3Reader s3, boolean validateChecksum, long maxSize) {
        this.s3 = s3;
        this.validateChecksum = validateChecksum;
        this.maxSize = maxSize;
    }

    public ResolvedRecord resolve(byte[] value) throws Exception {
        if (!LfsCodec.isEnvelope(value)) {
            return new ResolvedRecord(null, value, false);
        }
        if (s3 == null) {
            throw new IllegalStateException("s3 reader not configured");
        }
        LfsEnvelope env = LfsCodec.decode(value);
        byte[] payload = s3.fetch(env.key);
        if (maxSize > 0 && payload.length > maxSize) {
            throw new IllegalStateException("payload exceeds max size");
        }
        if (validateChecksum) {
            String expected = env.checksum != null && !env.checksum.isEmpty() ? env.checksum : env.sha256;
            String actual = Checksum.sha256(payload);
            if (!actual.equals(expected)) {
                throw new IllegalStateException("checksum mismatch");
            }
        }
        return new ResolvedRecord(env, payload, true);
    }

    public static final class ResolvedRecord {
        public final LfsEnvelope envelope;
        public final byte[] payload;
        public final boolean isEnvelope;

        public ResolvedRecord(LfsEnvelope envelope, byte[] payload, boolean isEnvelope) {
            this.envelope = envelope;
            this.payload = payload;
            this.isEnvelope = isEnvelope;
        }
    }
}
