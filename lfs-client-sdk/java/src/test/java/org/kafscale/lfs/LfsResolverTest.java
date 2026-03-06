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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LfsResolverTest {
    @Test
    void resolvesNonEnvelopeAsPlainPayload() throws Exception {
        byte[] payload = "plain".getBytes(StandardCharsets.UTF_8);
        LfsResolver resolver = new LfsResolver(new StaticS3Reader(payload), true, 0);

        LfsResolver.ResolvedRecord record = resolver.resolve(payload);

        assertFalse(record.isEnvelope);
        assertArrayEquals(payload, record.payload);
    }

    @Test
    void resolvesEnvelopeFromS3Reader() throws Exception {
        byte[] payload = "hello-lfs".getBytes(StandardCharsets.UTF_8);
        String checksum = Checksum.sha256(payload);
        String envelope = "{\"kfs_lfs\":1,\"bucket\":\"b\",\"key\":\"k\",\"sha256\":\"" + checksum + "\"}";

        LfsResolver resolver = new LfsResolver(new StaticS3Reader(payload), true, 0);
        LfsResolver.ResolvedRecord record = resolver.resolve(envelope.getBytes(StandardCharsets.UTF_8));

        assertTrue(record.isEnvelope);
        assertArrayEquals(payload, record.payload);
    }

    @Test
    void rejectsChecksumMismatch() {
        byte[] payload = "bad".getBytes(StandardCharsets.UTF_8);
        String envelope = "{\"kfs_lfs\":1,\"bucket\":\"b\",\"key\":\"k\",\"sha256\":\"deadbeef\"}";

        LfsResolver resolver = new LfsResolver(new StaticS3Reader(payload), true, 0);

        assertThrows(IllegalStateException.class,
                () -> resolver.resolve(envelope.getBytes(StandardCharsets.UTF_8)));
    }

    private static final class StaticS3Reader implements S3Reader {
        private final byte[] payload;

        private StaticS3Reader(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public byte[] fetch(String key) {
            return payload;
        }
    }
}
