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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LfsProducerTest {
    @Test
    void producesEnvelopeFromHttpResponse() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/lfs/produce", new OkHandler());
        server.start();
        try {
            URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort() + "/lfs/produce");
            LfsProducer producer = new LfsProducer(endpoint);

            byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
            LfsEnvelope env = producer.produce("demo-topic", null, new ByteArrayInputStream(payload), Map.of());

            assertEquals("demo-bucket", env.bucket);
            assertEquals("obj-1", env.key);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void failsOnNon2xx() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/lfs/produce", new ErrorHandler());
        server.start();
        try {
            URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort() + "/lfs/produce");
            LfsProducer producer = new LfsProducer(endpoint);

            assertThrows(LfsHttpException.class,
                    () -> producer.produce("demo-topic", null, new ByteArrayInputStream(new byte[0]), Map.of()));
        } finally {
            server.stop(0);
        }
    }


    @Test
    void retriesOnServerError() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/lfs/produce", exchange -> {
            int n = attempts.incrementAndGet();
            if (n < 3) {
                byte[] body = "boom".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(500, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
                return;
            }
            byte[] body = "{\"kfs_lfs\":1,\"bucket\":\"demo-bucket\",\"key\":\"obj-1\",\"sha256\":\"abc\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        try {
            URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort() + "/lfs/produce");
            LfsProducer producer = new LfsProducer(endpoint);

            LfsEnvelope env = producer.produce("demo-topic", null, new ByteArrayInputStream(new byte[0]), Map.of());

            assertEquals("demo-bucket", env.bucket);
            assertEquals(3, attempts.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void doesNotRetryOnClientError() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/lfs/produce", exchange -> {
            attempts.incrementAndGet();
            byte[] body = "bad".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(400, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        try {
            URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort() + "/lfs/produce");
            LfsProducer producer = new LfsProducer(endpoint);

            assertThrows(LfsHttpException.class,
                    () -> producer.produce("demo-topic", null, new ByteArrayInputStream(new byte[0]), Map.of()));
            assertEquals(1, attempts.get());
        } finally {
            server.stop(0);
        }
    }

    private static final class OkHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"demo-topic".equals(exchange.getRequestHeaders().getFirst("X-Kafka-Topic"))) {
                exchange.sendResponseHeaders(400, 0);
                exchange.close();
                return;
            }
            byte[] body = "{\"kfs_lfs\":1,\"bucket\":\"demo-bucket\",\"key\":\"obj-1\",\"sha256\":\"abc\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }


    @Test
    void honorsRequestTimeout() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/lfs/produce", new SlowHandler());
        server.start();
        try {
            URI endpoint = URI.create("http://localhost:" + server.getAddress().getPort() + "/lfs/produce");
            LfsProducer producer = new LfsProducer(endpoint, Duration.ofSeconds(1), Duration.ofMillis(50));

            assertThrows(java.net.http.HttpTimeoutException.class,
                    () -> producer.produce("demo-topic", null, new ByteArrayInputStream(new byte[0]), Map.of()));
        } finally {
            server.stop(0);
        }
    }

    private static final class ErrorHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] body = "boom".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }

    private static final class SlowHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                Thread.sleep(5000); // Sleep longer than the request timeout
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            byte[] body = "{\"kfs_lfs\":1,\"bucket\":\"demo-bucket\",\"key\":\"obj-1\",\"sha256\":\"abc\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}
