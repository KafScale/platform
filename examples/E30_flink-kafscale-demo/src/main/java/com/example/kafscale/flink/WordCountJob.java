package com.example.kafscale.flink;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WordCountJob {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountJob.class);

    private WordCountJob() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.load(args);

        logBanner();
        LOG.info("KafScale Flink config: bootstrapServers={}, topic={}, groupId={}, offsets={}",
                config.bootstrapServers, config.topic, config.groupId, config.startingOffsetsKind());
        LOG.info("Kafka compatibility: api.version.request={}, broker.version.fallback={}",
                config.kafkaApiVersionRequest, config.kafkaBrokerFallback);
        LOG.info("KafScale config env: KAFSCALE_BOOTSTRAP_SERVERS={}, KAFSCALE_TOPIC={}, KAFSCALE_GROUP_ID={}, KAFSCALE_STARTING_OFFSETS={}",
                Config.envOrDefault("KAFSCALE_BOOTSTRAP_SERVERS", "(unset)"),
                Config.envOrDefault("KAFSCALE_TOPIC", "(unset)"),
                Config.envOrDefault("KAFSCALE_GROUP_ID", "(unset)"),
                Config.envOrDefault("KAFSCALE_STARTING_OFFSETS", "(unset)"));
        preflightKafka(config);

        Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(RestOptions.PORT, config.flinkRestPort);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);

        Properties kafkaProps = new Properties();
        kafkaProps.put("api.version.request", config.kafkaApiVersionRequest);
        kafkaProps.put("broker.version.fallback", config.kafkaBrokerFallback);

        KafkaSource<CountEvent> source = KafkaSource
                .<CountEvent>builder()
                .setBootstrapServers(config.bootstrapServers)
                .setGroupId(config.groupId)
                .setTopics(config.topic)
                .setStartingOffsets(config.startingOffsets)
                .setDeserializer(new CountEventDeserializationSchema())
                .setProperties(kafkaProps)
                .build();

        DataStream<CountEvent> counts = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafscale-kafka-source")
                .keyBy(new CountKeySelector())
                .sum("count");

        counts.map(new CountEventFormatter()).print();

        env.execute("KafScale Flink WordCount Demo");
    }

    static final class Config {
        final String bootstrapServers;
        final String topic;
        final String groupId;
        final OffsetsInitializer startingOffsets;
        final String startingOffsetsLabel;
        final String kafkaApiVersionRequest;
        final String kafkaBrokerFallback;
        final int flinkRestPort;

        private Config(String bootstrapServers, String topic, String groupId, OffsetsInitializer startingOffsets,
                String startingOffsetsLabel, String kafkaApiVersionRequest, String kafkaBrokerFallback,
                int flinkRestPort) {
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.groupId = groupId;
            this.startingOffsets = startingOffsets;
            this.startingOffsetsLabel = startingOffsetsLabel;
            this.kafkaApiVersionRequest = kafkaApiVersionRequest;
            this.kafkaBrokerFallback = kafkaBrokerFallback;
            this.flinkRestPort = flinkRestPort;
        }

        static Config load(String[] args) {
            String profile = readProfile(args);
            Properties props = new Properties();
            if (!loadFromConfigDir(props, profile)) {
                loadFromClasspath(props, profile);
            }

            String bootstrapServers = envOrDefault("KAFSCALE_BOOTSTRAP_SERVERS",
                    props.getProperty("kafscale.bootstrap.servers", "localhost:39092"));
            String topic = envOrDefault("KAFSCALE_TOPIC", props.getProperty("kafscale.topic", "demo-topic-1"));
            String groupId = envOrDefault("KAFSCALE_GROUP_ID", props.getProperty("kafscale.group.id", "flink-wordcount-demo"));
            String offsets = envOrDefault("KAFSCALE_STARTING_OFFSETS",
                    props.getProperty("kafscale.starting.offsets", "latest"));
            String apiVersionRequest = envOrDefault("KAFSCALE_KAFKA_API_VERSION_REQUEST",
                    props.getProperty("kafscale.kafka.api.version.request", "false"));
            String brokerFallback = envOrDefault("KAFSCALE_KAFKA_BROKER_FALLBACK",
                    props.getProperty("kafscale.kafka.broker.version.fallback", "0.9.0.0"));
            String restPortValue = envOrDefault("KAFSCALE_FLINK_REST_PORT",
                    props.getProperty("kafscale.flink.rest.port", "8081"));
            int restPort = parseInt(restPortValue, 8081);

            boolean earliest = "earliest".equalsIgnoreCase(offsets);
            OffsetsInitializer initializer = earliest ? OffsetsInitializer.earliest() : OffsetsInitializer.latest();
            String label = earliest ? "earliest" : "latest";

            return new Config(bootstrapServers, topic, groupId, initializer, label, apiVersionRequest, brokerFallback, restPort);
        }

        String startingOffsetsKind() {
            return startingOffsetsLabel;
        }

        private static String readProfile(String[] args) {
            String envProfile = System.getenv("KAFSCALE_PROFILE");
            if (envProfile != null && !envProfile.isBlank()) {
                return envProfile.trim();
            }
            for (String arg : args) {
                if (arg != null && arg.startsWith("--profile=")) {
                    return arg.substring("--profile=".length()).trim();
                }
            }
            return "default";
        }

        static String envOrDefault(String key, String fallback) {
            String value = System.getenv(key);
            return (value == null || value.isBlank()) ? fallback : value.trim();
        }

        private static int parseInt(String value, int fallback) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                return fallback;
            }
        }

        private static boolean loadFromConfigDir(Properties props, String profile) {
            String configDir = System.getenv("KAFSCALE_CONFIG_DIR");
            if (configDir == null || configDir.isBlank()) {
                return false;
            }
            Path basePath = Path.of(configDir, "application.properties");
            if (!Files.exists(basePath)) {
                return false;
            }
            try (InputStream base = Files.newInputStream(basePath)) {
                props.load(base);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load " + basePath, e);
            }
            if (profile != null && !"default".equals(profile)) {
                Path overlayPath = Path.of(configDir, "application-" + profile + ".properties");
                if (Files.exists(overlayPath)) {
                    try (InputStream overlay = Files.newInputStream(overlayPath)) {
                        props.load(overlay);
                    } catch (IOException e) {
                        throw new IllegalStateException("Failed to load " + overlayPath, e);
                    }
                }
            }
            return true;
        }

        private static void loadFromClasspath(Properties props, String profile) {
            try (InputStream base = WordCountJob.class.getClassLoader().getResourceAsStream("application.properties")) {
                if (base != null) {
                    props.load(base);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load application.properties", e);
            }

            if (profile != null && !"default".equals(profile)) {
                String profileFile = "application-" + profile + ".properties";
                try (InputStream overlay = WordCountJob.class.getClassLoader().getResourceAsStream(profileFile)) {
                    if (overlay != null) {
                        props.load(overlay);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to load " + profileFile, e);
                }
            }
        }
    }

    static final class CountEventDeserializationSchema implements KafkaRecordDeserializationSchema<CountEvent> {
        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CountEvent> out) {
            emitCountEvents(record, out);
        }

        @Override
        public TypeInformation<CountEvent> getProducedType() {
            return TypeInformation.of(new TypeHint<CountEvent>() {
            });
        }
    }

    private static void emitCountEvents(ConsumerRecord<byte[], byte[]> record, Collector<CountEvent> out) {
        Headers headers = record.headers();
        if (headers == null || !headers.iterator().hasNext()) {
            out.collect(CountEvent.stat("no-header"));
        } else {
            for (Header header : headers) {
                emitWords("header", header.key(), out);
                byte[] value = header.value();
                if (value != null && value.length > 0) {
                    emitWords("header", new String(value, StandardCharsets.UTF_8), out);
                }
            }
        }

        byte[] key = record.key();
        if (key == null || key.length == 0) {
            out.collect(CountEvent.stat("no-key"));
        } else {
            emitWords("key", new String(key, StandardCharsets.UTF_8), out);
        }

        byte[] value = record.value();
        if (value == null || value.length == 0) {
            out.collect(CountEvent.stat("no-value"));
        } else {
            emitWords("value", new String(value, StandardCharsets.UTF_8), out);
        }
    }

    private static void emitWords(String category, String text, Collector<CountEvent> out) {
        for (String word : splitWords(text)) {
            out.collect(new CountEvent(category, word, 1));
        }
    }

    private static List<String> splitWords(String text) {
        if (text == null) {
            return List.of();
        }
        String normalized = text.toLowerCase(Locale.ROOT);
        String[] parts = normalized.split("[^a-z0-9]+");
        List<String> words = new ArrayList<>();
        for (String part : parts) {
            if (!part.isBlank()) {
                words.add(part);
            }
        }
        return words;
    }

    static final class CountKeySelector implements KeySelector<CountEvent, String> {
        @Override
        public String getKey(CountEvent value) {
            return value.category + ":" + value.token;
        }
    }

    static final class CountEventFormatter implements MapFunction<CountEvent, String> {
        @Override
        public String map(CountEvent value) {
            return value.category + " | " + value.token + " => " + value.count;
        }
    }

    public static final class CountEvent {
        public String category;
        public String token;
        public long count;

        public CountEvent() {
        }

        public CountEvent(String category, String token, long count) {
            this.category = Objects.requireNonNull(category, "category");
            this.token = Objects.requireNonNull(token, "token");
            this.count = count;
        }

        static CountEvent stat(String token) {
            return new CountEvent("stats", token, 1);
        }
    }

    private static void logBanner() {
        System.out.println("==================================================");
        System.out.println("   _  __      __   _____           __        ");
        System.out.println("  | |/ /___ _/ /  / ___/__________/ /__ ___  ");
        System.out.println("  |   / __ `/ /   \\__ \\/ ___/ ___/ / _ ` _ \\ ");
        System.out.println(" /   / /_/ / /   ___/ / /__/ /__/ /  __/  __/ ");
        System.out.println("/_/|_\\__,_/_/   /____/\\___/\\___/_/\\___/_/    ");
        System.out.println("        Flink WordCount Demo (E30)");
        System.out.println("==================================================");
    }

    private static void preflightKafka(Config config) {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", config.bootstrapServers);
        adminProps.put("api.version.request", config.kafkaApiVersionRequest);
        adminProps.put("broker.version.fallback", config.kafkaBrokerFallback);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            DescribeClusterResult cluster = admin.describeCluster();
            String clusterId = cluster.clusterId().get(5, TimeUnit.SECONDS);
            List<Node> nodes = new ArrayList<>(cluster.nodes().get(5, TimeUnit.SECONDS));
            LOG.info("Kafka cluster: id={}, nodes={}", clusterId, nodes);

            ListTopicsResult topicsResult = admin.listTopics();
            List<String> topicNames = new ArrayList<>(topicsResult.names().get(5, TimeUnit.SECONDS));
            LOG.info("Kafka topics visible to job: {}", topicNames);

            if (!topicNames.isEmpty()) {
                DescribeTopicsResult describe = admin.describeTopics(topicNames);
                Map<String, TopicDescription> descriptions = describe.all().get(5, TimeUnit.SECONDS);
                for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                    TopicDescription desc = entry.getValue();
                    LOG.info("Topic metadata: name={}, partitions={}, internal={}, authorizedOps={}",
                            desc.name(), desc.partitions().size(), desc.isInternal(), desc.authorizedOperations());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Kafka preflight failed. Check bootstrap server and connectivity.", e);
        }
    }
}
