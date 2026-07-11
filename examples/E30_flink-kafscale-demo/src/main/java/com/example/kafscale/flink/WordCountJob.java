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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
        configureRuntime(env, config);
        env.setParallelism(1);

        Properties kafkaProps = new Properties();
        kafkaProps.put("api.version.request", config.kafkaApiVersionRequest);
        kafkaProps.put("broker.version.fallback", config.kafkaBrokerFallback);
        kafkaProps.put(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), String.valueOf(config.commitOnCheckpoint));
        kafkaProps.put("max.poll.interval.ms", String.valueOf(config.maxPollIntervalMs));
        kafkaProps.put("max.poll.records", String.valueOf(config.maxPollRecords));
        kafkaProps.put("session.timeout.ms", String.valueOf(config.sessionTimeoutMs));
        kafkaProps.put("heartbeat.interval.ms", String.valueOf(config.heartbeatIntervalMs));

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

        DataStream<String> output = counts.map(new CountEventFormatter());
        output.print();

        if (config.sinkEnabled) {
            Properties sinkProps = new Properties();
            sinkProps.put("enable.idempotence", String.valueOf(config.sinkEnableIdempotence));
            DeliveryGuarantee deliveryGuarantee = config.sinkDeliveryGuarantee;
            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(config.bootstrapServers)
                    .setKafkaProducerConfig(sinkProps)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(config.sinkTopic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(deliveryGuarantee)
                    .build();
            output.sinkTo(sink).name("kafka-counts-sink");
        }

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
        final boolean commitOnCheckpoint;
        final int maxPollIntervalMs;
        final int maxPollRecords;
        final int sessionTimeoutMs;
        final int heartbeatIntervalMs;
        final long checkpointIntervalMs;
        final long checkpointMinPauseMs;
        final long checkpointTimeoutMs;
        final String checkpointDir;
        final String stateBackend;
        final int restartAttempts;
        final long restartDelayMs;
        final boolean sinkEnabled;
        final String sinkTopic;
        final boolean sinkEnableIdempotence;
        final DeliveryGuarantee sinkDeliveryGuarantee;

        private Config(String bootstrapServers, String topic, String groupId, OffsetsInitializer startingOffsets,
                String startingOffsetsLabel, String kafkaApiVersionRequest, String kafkaBrokerFallback,
                int flinkRestPort, boolean commitOnCheckpoint, int maxPollIntervalMs, int maxPollRecords,
                int sessionTimeoutMs, int heartbeatIntervalMs, long checkpointIntervalMs, long checkpointMinPauseMs,
                long checkpointTimeoutMs, String checkpointDir, String stateBackend, int restartAttempts,
                long restartDelayMs, boolean sinkEnabled, String sinkTopic, boolean sinkEnableIdempotence,
                DeliveryGuarantee sinkDeliveryGuarantee) {
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.groupId = groupId;
            this.startingOffsets = startingOffsets;
            this.startingOffsetsLabel = startingOffsetsLabel;
            this.kafkaApiVersionRequest = kafkaApiVersionRequest;
            this.kafkaBrokerFallback = kafkaBrokerFallback;
            this.flinkRestPort = flinkRestPort;
            this.commitOnCheckpoint = commitOnCheckpoint;
            this.maxPollIntervalMs = maxPollIntervalMs;
            this.maxPollRecords = maxPollRecords;
            this.sessionTimeoutMs = sessionTimeoutMs;
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.checkpointIntervalMs = checkpointIntervalMs;
            this.checkpointMinPauseMs = checkpointMinPauseMs;
            this.checkpointTimeoutMs = checkpointTimeoutMs;
            this.checkpointDir = checkpointDir;
            this.stateBackend = stateBackend;
            this.restartAttempts = restartAttempts;
            this.restartDelayMs = restartDelayMs;
            this.sinkEnabled = sinkEnabled;
            this.sinkTopic = sinkTopic;
            this.sinkEnableIdempotence = sinkEnableIdempotence;
            this.sinkDeliveryGuarantee = sinkDeliveryGuarantee;
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
            boolean commitOnCheckpoint = Boolean.parseBoolean(envOrDefault("KAFSCALE_COMMIT_ON_CHECKPOINT",
                    props.getProperty("kafscale.commit.on.checkpoint", "false")));
            int maxPollIntervalMs = parseInt(envOrDefault("KAFSCALE_CONSUMER_MAX_POLL_INTERVAL_MS",
                    props.getProperty("kafscale.consumer.max.poll.interval.ms", "300000")), 300000);
            int maxPollRecords = parseInt(envOrDefault("KAFSCALE_CONSUMER_MAX_POLL_RECORDS",
                    props.getProperty("kafscale.consumer.max.poll.records", "500")), 500);
            int sessionTimeoutMs = parseInt(envOrDefault("KAFSCALE_CONSUMER_SESSION_TIMEOUT_MS",
                    props.getProperty("kafscale.consumer.session.timeout.ms", "30000")), 30000);
            int heartbeatIntervalMs = parseInt(envOrDefault("KAFSCALE_CONSUMER_HEARTBEAT_INTERVAL_MS",
                    props.getProperty("kafscale.consumer.heartbeat.interval.ms", "10000")), 10000);
            long checkpointInterval = parseLong(envOrDefault("KAFSCALE_CHECKPOINT_INTERVAL_MS",
                    props.getProperty("kafscale.checkpoint.interval.ms", "10000")), 10000L);
            long checkpointMinPause = parseLong(envOrDefault("KAFSCALE_CHECKPOINT_MIN_PAUSE_MS",
                    props.getProperty("kafscale.checkpoint.min.pause.ms", "3000")), 3000L);
            long checkpointTimeout = parseLong(envOrDefault("KAFSCALE_CHECKPOINT_TIMEOUT_MS",
                    props.getProperty("kafscale.checkpoint.timeout.ms", "60000")), 60000L);
            String checkpointDir = envOrDefault("KAFSCALE_CHECKPOINT_DIR",
                    props.getProperty("kafscale.checkpoint.dir", "file:///tmp/kafscale-flink-checkpoints"));
            String stateBackend = envOrDefault("KAFSCALE_STATE_BACKEND",
                    props.getProperty("kafscale.state.backend", "hashmap"));
            int restartAttempts = parseInt(envOrDefault("KAFSCALE_RESTART_ATTEMPTS",
                    props.getProperty("kafscale.restart.attempts", "3")), 3);
            long restartDelayMs = parseLong(envOrDefault("KAFSCALE_RESTART_DELAY_MS",
                    props.getProperty("kafscale.restart.delay.ms", "5000")), 5000L);
            boolean sinkEnabled = Boolean.parseBoolean(envOrDefault("KAFSCALE_SINK_ENABLED",
                    props.getProperty("kafscale.sink.enabled", "true")));
            String sinkTopic = envOrDefault("KAFSCALE_SINK_TOPIC",
                    props.getProperty("kafscale.sink.topic", "demo-topic-1-counts"));
            boolean sinkEnableIdempotence = Boolean.parseBoolean(envOrDefault("KAFSCALE_SINK_ENABLE_IDEMPOTENCE",
                    props.getProperty("kafscale.sink.enable.idempotence", "false")));
            String sinkGuaranteeValue = envOrDefault("KAFSCALE_SINK_DELIVERY_GUARANTEE",
                    props.getProperty("kafscale.sink.delivery.guarantee", "none"));
            DeliveryGuarantee sinkGuarantee = parseDeliveryGuarantee(sinkGuaranteeValue, sinkEnableIdempotence);

            boolean earliest = "earliest".equalsIgnoreCase(offsets);
            OffsetsInitializer initializer = earliest ? OffsetsInitializer.earliest() : OffsetsInitializer.latest();
            String label = earliest ? "earliest" : "latest";

            return new Config(bootstrapServers, topic, groupId, initializer, label, apiVersionRequest, brokerFallback,
                    restPort, commitOnCheckpoint, maxPollIntervalMs, maxPollRecords, sessionTimeoutMs,
                    heartbeatIntervalMs, checkpointInterval, checkpointMinPause, checkpointTimeout, checkpointDir,
                    stateBackend, restartAttempts, restartDelayMs, sinkEnabled, sinkTopic, sinkEnableIdempotence,
                    sinkGuarantee);
        }

        String startingOffsetsKind() {
            return startingOffsetsLabel;
        }

        private static String readProfile(String[] args) {
            String setupProfile = System.getenv("KAFSCALE_SETUP_PROFILE");
            if (setupProfile != null && !setupProfile.isBlank()) {
                return setupProfile.trim();
            }
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

        private static long parseLong(String value, long fallback) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException ex) {
                return fallback;
            }
        }

        private static DeliveryGuarantee parseDeliveryGuarantee(String value, boolean idempotenceEnabled) {
            if (value == null) {
                return DeliveryGuarantee.NONE;
            }
            String normalized = value.trim().toLowerCase(Locale.ROOT);
            if ("at-least-once".equals(normalized) || "at_least_once".equals(normalized)) {
                return idempotenceEnabled ? DeliveryGuarantee.AT_LEAST_ONCE : DeliveryGuarantee.NONE;
            }
            return DeliveryGuarantee.NONE;
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

    private static void configureRuntime(StreamExecutionEnvironment env, Config config) {
        if (config.checkpointIntervalMs > 0) {
            env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.AT_LEAST_ONCE);
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
            checkpointConfig.setMinPauseBetweenCheckpoints(config.checkpointMinPauseMs);
            checkpointConfig.setCheckpointTimeout(config.checkpointTimeoutMs);
            if (config.checkpointDir != null && !config.checkpointDir.isBlank()) {
                checkpointConfig.setCheckpointStorage(config.checkpointDir);
            }
        }
        if ("rocksdb".equalsIgnoreCase(config.stateBackend)) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
        }
        if (config.restartAttempts > 0) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    config.restartAttempts,
                    org.apache.flink.api.common.time.Time.milliseconds(config.restartDelayMs)));
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
