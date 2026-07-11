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

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LfsProducer {

    private static class ErrorResponse {
        public String code;
        public String message;
        public String request_id;
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long MULTIPART_MIN_BYTES = 5L * 1024 * 1024;
    private static final String HEADER_REQUEST_ID = "X-Request-ID";
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(5);
    private static final int DEFAULT_RETRIES = 3;
    private static final long RETRY_BASE_SLEEP_MILLIS = 200L;

    private final HttpClient client;
    private final URI endpoint;
    private final Duration requestTimeout;

    public LfsProducer(URI endpoint) {
        this(endpoint, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT);
    }

    public LfsProducer(URI endpoint, Duration connectTimeout, Duration requestTimeout) {
        Duration resolvedConnect = connectTimeout == null ? DEFAULT_CONNECT_TIMEOUT : connectTimeout;
        Duration resolvedRequest = requestTimeout == null ? DEFAULT_REQUEST_TIMEOUT : requestTimeout;
        this.client = HttpClient.newBuilder()
                .connectTimeout(resolvedConnect)
                .build();
        this.endpoint = endpoint;
        this.requestTimeout = resolvedRequest;
    }

    public LfsEnvelope produce(String topic, byte[] key, InputStream payload, Map<String, String> headers) throws Exception {
        return produce(topic, key, payload, headers, -1);
    }

    public LfsEnvelope produce(String topic, byte[] key, InputStream payload, Map<String, String> headers, long sizeHint) throws Exception {
        // Read InputStream into byte array to ensure proper Content-Length and retry support
        byte[] data = payload.readAllBytes();

        Map<String, String> outHeaders = new HashMap<>();
        outHeaders.put("X-Kafka-Topic", topic);
        if (key != null) {
            outHeaders.put("X-Kafka-Key", new String(key));
        }
        if (headers != null) {
            outHeaders.putAll(headers);
        }
        if (!outHeaders.containsKey(HEADER_REQUEST_ID)) {
            outHeaders.put(HEADER_REQUEST_ID, UUID.randomUUID().toString());
        }
        long actualSize = data.length;
        outHeaders.put("X-LFS-Size", String.valueOf(actualSize));
        outHeaders.put("X-LFS-Mode", actualSize < MULTIPART_MIN_BYTES ? "single" : "multipart");

        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(requestTimeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(data));

        for (Map.Entry<String, String> entry : outHeaders.entrySet()) {
            req.header(entry.getKey(), entry.getValue());
        }

        return sendWithRetry(req.build());
    }

    public LfsEnvelope produce(String topic, byte[] key, byte[] data, Map<String, String> headers) throws Exception {
        return produce(topic, key, new java.io.ByteArrayInputStream(data), headers, data.length);
    }

    private LfsEnvelope sendWithRetry(HttpRequest request) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= DEFAULT_RETRIES; attempt++) {
            try {
                HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                    String body = resp.body();
                    String requestId = resp.headers().firstValue(HEADER_REQUEST_ID).orElse("");
                    ErrorResponse err = null;
                    try {
                        err = MAPPER.readValue(body, ErrorResponse.class);
                    } catch (Exception ignored) {
                    }
                    String code = err != null ? err.code : "";
                    String message = err != null && err.message != null ? err.message : body;
                    String errRequestId = err != null && err.request_id != null ? err.request_id : requestId;
                    LfsHttpException httpError = new LfsHttpException(resp.statusCode(), code, message, errRequestId, body);
                    if (resp.statusCode() >= 500 && attempt < DEFAULT_RETRIES) {
                        last = httpError;
                        sleepBackoff(attempt);
                        continue;
                    }
                    throw httpError;
                }
                return MAPPER.readValue(resp.body(), LfsEnvelope.class);
            } catch (java.io.IOException ex) {
                last = ex;
                if (attempt == DEFAULT_RETRIES) {
                    break;
                }
                sleepBackoff(attempt);
            }
        }
        if (last != null) {
            throw last;
        }
        throw new IllegalStateException("produce failed: no response");
    }

    private void sleepBackoff(int attempt) {
        try {
            Thread.sleep(RETRY_BASE_SLEEP_MILLIS * (1L << (attempt - 1)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
