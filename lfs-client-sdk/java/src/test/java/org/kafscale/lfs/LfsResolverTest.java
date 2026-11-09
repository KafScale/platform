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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @ParameterizedTest
    @ValueSource(strings = {"sha256", "md5", "crc32", "none"})
    void resolvesValidEnvelopeForEveryChecksumAlgorithm(String alg) throws Exception {
        byte[] payload = largePayload();
        LfsResolver resolver = new LfsResolver(new StaticS3Reader(payload), true, 0);

        LfsResolver.ResolvedRecord record = resolver.resolve(algorithmEnvelope(alg, payload));

        assertTrue(record.isEnvelope);
        assertArrayEquals(payload, record.payload);
    }

    @ParameterizedTest
    @ValueSource(strings = {"sha256", "md5", "crc32", "none"})
    void rejectsCorruptObjectForEveryChecksumAlgorithm(String alg) throws Exception {
        byte[] payload = largePayload();
        byte[] corrupt = payload.clone();
        corrupt[0] ^= 1;
        LfsResolver resolver = new LfsResolver(new StaticS3Reader(corrupt), true, 0);

        IllegalStateException err = assertThrows(IllegalStateException.class,
                () -> resolver.resolve(algorithmEnvelope(alg, payload)));
        assertEquals("checksum mismatch", err.getMessage());
    }

    @Test
    void rejectsUnsupportedChecksumAlgorithm() {
        byte[] payload = largePayload();
        String envelope = "{\"kfs_lfs\":1,\"bucket\":\"b\",\"key\":\"k\",\"sha256\":\"" + Checksum.sha256(payload)
                + "\",\"checksum\":\"x\",\"checksum_alg\":\"sha1\"}";
        LfsResolver resolver = new LfsResolver(new StaticS3Reader(payload), true, 0);

        IllegalStateException err = assertThrows(IllegalStateException.class,
                () -> resolver.resolve(envelope.getBytes(StandardCharsets.UTF_8)));
        assertEquals("unsupported checksum algorithm: sha1", err.getMessage());
    }

    @Test
    void rejectsEmptySha256BeforeFetching() {
        String envelope = "{\"kfs_lfs\":1,\"bucket\":\"b\",\"key\":\"k\",\"sha256\":\"\"}";
        LfsResolver resolver = new LfsResolver(key -> {
            throw new AssertionError("unexpected S3 fetch");
        }, true, 0);

        java.io.IOException err = assertThrows(java.io.IOException.class,
                () -> resolver.resolve(envelope.getBytes(StandardCharsets.UTF_8)));
        assertEquals("invalid envelope: missing required fields", err.getMessage());
    }

    private static byte[] largePayload() {
        byte[] payload = new byte[262144];
        Arrays.fill(payload, (byte) 42);
        return payload;
    }

    private static byte[] algorithmEnvelope(String alg, byte[] payload) throws Exception {
        String checksum = switch (alg) {
            case "sha256" -> Checksum.sha256(payload);
            case "md5" -> HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(payload));
            case "crc32" -> {
                CRC32 crc = new CRC32();
                crc.update(payload);
                yield String.format("%08x", crc.getValue());
            }
            default -> "";
        };
        String checksumField = checksum.isEmpty() ? "" : ",\"checksum\":\"" + checksum + "\"";
        String envelope = "{\"kfs_lfs\":1,\"bucket\":\"b\",\"key\":\"k\",\"sha256\":\"" + Checksum.sha256(payload)
                + "\"" + checksumField + ",\"checksum_alg\":\"" + alg + "\"}";
        return envelope.getBytes(StandardCharsets.UTF_8);
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
