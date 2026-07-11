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
import org.testcontainers.DockerClientFactory;

class DockerAvailabilityTest {
    @Test
    void printsDockerAvailability() {
        System.err.println("[tc-diag] DOCKER_HOST=" + System.getenv("DOCKER_HOST"));
        System.err.println("[tc-diag] DOCKER_CONTEXT=" + System.getenv("DOCKER_CONTEXT"));
        System.err.println("[tc-diag] TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=" + System.getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"));
        System.err.println("[tc-diag] testcontainers.docker.socket.override=" + System.getProperty("testcontainers.docker.socket.override"));
        try {
            System.err.println("[tc-diag] config.dockerHost=" + readConfigValue("docker.host"));
            System.err.println("[tc-diag] factoryMethods=" + listFactoryMethods());
            DockerClientFactory factory = DockerClientFactory.instance();
            tryInvokeClient(factory);
            tryInvokeStrategy(factory);
            try {
                System.err.println("[tc-diag] dockerAvailable=" + factory.isDockerAvailable());
            } catch (Exception e) {
                System.err.println("[tc-diag] dockerAvailable failed: " + e.getClass().getName() + ": " + e.getMessage());
            }
            try {
                System.err.println("[tc-diag] dockerHostIp=" + factory.dockerHostIpAddress());
            } catch (Exception e) {
                System.err.println("[tc-diag] dockerHostIp failed: " + e.getClass().getName() + ": " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("[tc-diag] docker check failed: " + e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private static String readConfigValue(String key) {
        String value = readClasspathConfig(key);
        if (value != null) {
            return value;
        }
        return readFileConfig(System.getProperty("user.home") + "/.testcontainers.properties", key);
    }

    private static String readClasspathConfig(String key) {
        try (var stream = DockerAvailabilityTest.class.getClassLoader().getResourceAsStream(".testcontainers.properties")) {
            if (stream == null) {
                return null;
            }
            java.util.Properties props = new java.util.Properties();
            props.load(stream);
            return props.getProperty(key);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String readFileConfig(String path, String key) {
        java.io.File file = new java.io.File(path);
        if (!file.exists()) {
            return null;
        }
        try (var stream = new java.io.FileInputStream(file)) {
            java.util.Properties props = new java.util.Properties();
            props.load(stream);
            return props.getProperty(key);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String listFactoryMethods() {
        java.lang.reflect.Method[] methods = DockerClientFactory.class.getDeclaredMethods();
        java.util.List<String> names = new java.util.ArrayList<>();
        for (java.lang.reflect.Method method : methods) {
            if (method.getParameterCount() == 0) {
                names.add(method.getName());
            }
        }
        java.util.Collections.sort(names);
        return String.join(",", names);
    }

    private static void tryInvokeClient(DockerClientFactory factory) {
        tryInvoke(factory, "client");
        tryInvoke(factory, "getClient");
    }

    private static void tryInvokeStrategy(DockerClientFactory factory) {
        try {
            java.lang.reflect.Method method = DockerClientFactory.class.getDeclaredMethod("getOrInitializeStrategy");
            method.setAccessible(true);
            Object strategy = method.invoke(factory);
            System.err.println("[tc-diag] strategy=" + (strategy == null ? "null" : strategy.getClass().getName()));
        } catch (Exception e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            System.err.println("[tc-diag] strategy failed: " + cause.getClass().getName() + ": " + cause.getMessage());
        }
    }

    private static void tryInvoke(DockerClientFactory factory, String methodName) {
        try {
            java.lang.reflect.Method method = DockerClientFactory.class.getDeclaredMethod(methodName);
            method.setAccessible(true);
            Object client = method.invoke(factory);
            System.err.println("[tc-diag] " + methodName + " ok: " + (client == null ? "null" : client.getClass().getName()));
        } catch (NoSuchMethodException ignored) {
            System.err.println("[tc-diag] " + methodName + " not found");
        } catch (Exception e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            System.err.println("[tc-diag] " + methodName + " failed: " + cause.getClass().getName() + ": " + cause.getMessage());
        }
    }
}
