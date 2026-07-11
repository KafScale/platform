package com.example.kafscale.spark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public final class WordCountSparkJob {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountSparkJob.class);

    private WordCountSparkJob() {
    }

    public static void main(String[] args) throws StreamingQueryException {
        Config config = Config.load(args);

        logBanner();
        LOG.info("KafScale Spark config: bootstrapServers={}, topic={}, groupId={}, offsets={}",
                config.bootstrapServers, config.topic, config.groupId, config.startingOffsets);
        LOG.info("Kafka compatibility: api.version.request={}, broker.version.fallback={}",
                config.kafkaApiVersionRequest, config.kafkaBrokerFallback);
        LOG.info("Spark UI port: {}", config.sparkUiPort);
        LOG.info("Checkpoint dir: {}", config.checkpointDir);
        LOG.info("Include Kafka headers: {}", config.includeHeaders);
        LOG.info("Fail on data loss: {}", config.failOnDataLoss);
        LOG.info("Delta enabled: {}", config.deltaEnabled);
        LOG.info("Delta path: {}", config.deltaPath);

        preflightKafka(config);

        SparkSession.Builder builder = SparkSession.builder()
                .appName("KafScale Spark WordCount Demo")
                .config("spark.ui.port", config.sparkUiPort);
        if (config.deltaEnabled) {
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
        }
        SparkSession spark = builder.getOrCreate();

        Dataset<Row> kafka = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.bootstrapServers)
                .option("subscribe", config.topic)
                .option("startingOffsets", config.startingOffsets)
                .option("kafka.group.id", config.groupId)
                .option("kafka.api.version.request", config.kafkaApiVersionRequest)
                .option("kafka.broker.version.fallback", config.kafkaBrokerFallback)
                .option("includeHeaders", String.valueOf(config.includeHeaders))
                .option("failOnDataLoss", String.valueOf(config.failOnDataLoss))
                .load();

        Dataset<Row> tokens = buildTokens(kafka, config.includeHeaders);

        Dataset<Row> counts = tokens.groupBy(col("category"), col("token")).count();

        StreamingQuery query;
        try {
            if (config.deltaEnabled) {
                query = counts.writeStream()
                        .outputMode("complete")
                        .format("delta")
                        .option("checkpointLocation", config.checkpointDir)
                        .option("path", config.deltaPath)
                        .start();
            } else {
                query = counts.writeStream()
                        .outputMode("complete")
                        .format("console")
                        .option("truncate", "false")
                        .option("checkpointLocation", config.checkpointDir)
                        .start();
            }
        } catch (TimeoutException e) {
            throw new IllegalStateException("Failed to start Spark query within timeout.", e);
        }

        query.awaitTermination();
    }

    private static Dataset<Row> buildTokens(Dataset<Row> kafka, boolean includeHeaders) {
        Column keyCol = col("key").cast("string");
        Column valueCol = col("value").cast("string");
        Column headersCol = includeHeaders ? col("headers") : lit(null);

        Dataset<Row> keyWords = wordsFromText(kafka, "key", keyCol);
        Dataset<Row> valueWords = wordsFromText(kafka, "value", valueCol);

        Dataset<Row> headerKeyWords = sparkSessionEmpty(kafka);
        Dataset<Row> headerValueWords = sparkSessionEmpty(kafka);
        if (includeHeaders) {
            Dataset<Row> headerRows = kafka.where(headersCol.isNotNull().and(size(headersCol).gt(0)))
                    .select(explode(headersCol).alias("header"));
            headerKeyWords = wordsFromText(headerRows, "header", col("header.key"));
            headerValueWords = wordsFromText(headerRows, "header", col("header.value").cast("string"));
        }

        Dataset<Row> stats = missingStats(kafka, keyCol, valueCol, headersCol, includeHeaders);

        return keyWords.union(valueWords).union(headerKeyWords).union(headerValueWords).union(stats)
                .where(col("token").isNotNull().and(length(col("token")).gt(0)));
    }

    private static Dataset<Row> wordsFromText(Dataset<Row> input, String category, Column textCol) {
        Column normalized = lower(regexp_replace(textCol, "[^a-zA-Z0-9]+", " "));
        Column tokens = split(normalized, " ");
        return input.where(textCol.isNotNull().and(length(textCol).gt(0)))
                .select(lit(category).alias("category"), explode(tokens).alias("token"));
    }

    private static Dataset<Row> missingStats(Dataset<Row> kafka, Column keyCol, Column valueCol, Column headersCol,
            boolean includeHeaders) {
        Column noKey = when(keyCol.isNull().or(length(keyCol).equalTo(0)), lit("no-key"));
        Column noValue = when(valueCol.isNull().or(length(valueCol).equalTo(0)), lit("no-value"));
        Column noHeader = includeHeaders
                ? when(headersCol.isNull().or(size(headersCol).equalTo(0)), lit("no-header"))
                : lit(null);

        Dataset<Row> base = kafka.select(noKey.alias("noKey"), noValue.alias("noValue"), noHeader.alias("noHeader"));

        Dataset<Row> noKeyRows = base.where(col("noKey").isNotNull())
                .select(lit("stats").alias("category"), col("noKey").alias("token"));
        Dataset<Row> noValueRows = base.where(col("noValue").isNotNull())
                .select(lit("stats").alias("category"), col("noValue").alias("token"));
        Dataset<Row> noHeaderRows = includeHeaders
                ? base.where(col("noHeader").isNotNull())
                        .select(lit("stats").alias("category"), col("noHeader").alias("token"))
                : sparkSessionEmpty(kafka);

        return noKeyRows.union(noValueRows).union(noHeaderRows);
    }

    private static Dataset<Row> sparkSessionEmpty(Dataset<Row> kafka) {
        return kafka.sparkSession().emptyDataFrame()
                .select(lit("").alias("category"), lit("").alias("token"))
                .limit(0);
    }

    private static void logBanner() {
        System.out.println("==================================================");
        System.out.println("   ____                      __                  ");
        System.out.println("  / __ \\____  ____ _________/ /__  _____         ");
        System.out.println(" / / / / __ \\/ __ `/ ___/ __  / _ \\/ ___/         ");
        System.out.println("/ /_/ / /_/ / /_/ / /  / /_/ /  __/ /             ");
        System.out.println("\\____/ .___/\\__,_/_/   \\__,_/\\___/_/              ");
        System.out.println("    /_/      Spark WordCount Demo (E40)            ");
        System.out.println("==================================================");
    }

    private static void preflightKafka(Config config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.bootstrapServers);
        props.put("api.version.request", config.kafkaApiVersionRequest);
        props.put("broker.version.fallback", config.kafkaBrokerFallback);

        try (AdminClient admin = AdminClient.create(props)) {
            DescribeClusterResult cluster = admin.describeCluster();
            String clusterId = cluster.clusterId().get();
            List<Node> nodes = new ArrayList<>(cluster.nodes().get());
            LOG.info("Kafka cluster: id={}, nodes={}", clusterId, nodes);

            ListTopicsResult topicsResult = admin.listTopics();
            List<String> topicNames = new ArrayList<>(topicsResult.names().get());
            LOG.info("Kafka topics visible to job: {}", topicNames);

            if (!topicNames.isEmpty()) {
                DescribeTopicsResult describe = admin.describeTopics(topicNames);
                for (var entry : describe.all().get().entrySet()) {
                    TopicDescription desc = entry.getValue();
                    LOG.info("Topic metadata: name={}, partitions={}, internal={}, authorizedOps={}",
                            desc.name(), desc.partitions().size(), desc.isInternal(), desc.authorizedOperations());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Kafka preflight failed. Check bootstrap server and connectivity.", e);
        }
    }

    static final class Config {
        final String bootstrapServers;
        final String topic;
        final String groupId;
        final String startingOffsets;
        final String kafkaApiVersionRequest;
        final String kafkaBrokerFallback;
        final boolean includeHeaders;
        final boolean failOnDataLoss;
        final boolean deltaEnabled;
        final String deltaPath;
        final String sparkUiPort;
        final String checkpointDir;

        private Config(String bootstrapServers, String topic, String groupId, String startingOffsets,
                String kafkaApiVersionRequest, String kafkaBrokerFallback, boolean includeHeaders, boolean failOnDataLoss,
                boolean deltaEnabled, String deltaPath, String sparkUiPort, String checkpointDir) {
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.groupId = groupId;
            this.startingOffsets = startingOffsets;
            this.kafkaApiVersionRequest = kafkaApiVersionRequest;
            this.kafkaBrokerFallback = kafkaBrokerFallback;
            this.includeHeaders = includeHeaders;
            this.failOnDataLoss = failOnDataLoss;
            this.deltaEnabled = deltaEnabled;
            this.deltaPath = deltaPath;
            this.sparkUiPort = sparkUiPort;
            this.checkpointDir = checkpointDir;
        }

        static Config load(String[] args) {
            String profile = readProfile(args);
            Properties props = new Properties();

            if (!loadFromConfigDir(props, profile)) {
                loadFromClasspath(props, profile);
            }

            String bootstrapServers = envOrDefault("KAFSCALE_BOOTSTRAP_SERVERS",
                    props.getProperty("kafscale.bootstrap.servers", "127.0.0.1:39092"));
            String topic = envOrDefault("KAFSCALE_TOPIC", props.getProperty("kafscale.topic", "demo-topic-1"));
            String groupId = envOrDefault("KAFSCALE_GROUP_ID", props.getProperty("kafscale.group.id", "spark-wordcount-demo"));
            String startingOffsets = envOrDefault("KAFSCALE_STARTING_OFFSETS",
                    props.getProperty("kafscale.starting.offsets", "latest"));
            String apiVersionRequest = envOrDefault("KAFSCALE_KAFKA_API_VERSION_REQUEST",
                    props.getProperty("kafscale.kafka.api.version.request", "false"));
            String brokerFallback = envOrDefault("KAFSCALE_KAFKA_BROKER_FALLBACK",
                    props.getProperty("kafscale.kafka.broker.version.fallback", "0.9.0.0"));
            boolean includeHeaders = Boolean.parseBoolean(envOrDefault("KAFSCALE_INCLUDE_HEADERS",
                    props.getProperty("kafscale.include.headers", "true")));
            boolean failOnDataLoss = Boolean.parseBoolean(envOrDefault("KAFSCALE_FAIL_ON_DATA_LOSS",
                    props.getProperty("kafscale.fail.on.data.loss", "false")));
            boolean deltaEnabled = Boolean.parseBoolean(envOrDefault("KAFSCALE_DELTA_ENABLED",
                    props.getProperty("kafscale.delta.enabled", "false")));
            String deltaPath = envOrDefault("KAFSCALE_DELTA_PATH",
                    props.getProperty("kafscale.delta.path", "/tmp/kafscale-delta-wordcount"));
            String sparkUiPort = envOrDefault("KAFSCALE_SPARK_UI_PORT",
                    props.getProperty("kafscale.spark.ui.port", "4040"));
            String checkpointDir = envOrDefault("KAFSCALE_CHECKPOINT_DIR",
                    props.getProperty("kafscale.checkpoint.dir", "/tmp/kafscale-spark-checkpoints"));

            return new Config(bootstrapServers, topic, groupId, startingOffsets, apiVersionRequest, brokerFallback,
                    includeHeaders, failOnDataLoss, deltaEnabled, deltaPath, sparkUiPort, checkpointDir);
        }

        private static String readProfile(String[] args) {
            String envProfile = System.getenv("KAFSCALE_SETUP_PROFILE");
            if (envProfile != null && !envProfile.isBlank()) {
                return envProfile.trim();
            }
            String legacyProfile = System.getenv("KAFSCALE_PROFILE");
            if (legacyProfile != null && !legacyProfile.isBlank()) {
                return legacyProfile.trim();
            }
            for (String arg : args) {
                if (arg != null && arg.startsWith("--profile=")) {
                    return arg.substring("--profile=".length()).trim();
                }
            }
            return "default";
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
            try (InputStream base = WordCountSparkJob.class.getClassLoader().getResourceAsStream("application.properties")) {
                if (base != null) {
                    props.load(base);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load application.properties", e);
            }

            if (profile != null && !"default".equals(profile)) {
                String profileFile = "application-" + profile + ".properties";
                try (InputStream overlay = WordCountSparkJob.class.getClassLoader().getResourceAsStream(profileFile)) {
                    if (overlay != null) {
                        props.load(overlay);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to load " + profileFile, e);
                }
            }
        }

        static String envOrDefault(String key, String fallback) {
            String value = System.getenv(key);
            return (value == null || value.isBlank()) ? fallback : value.trim();
        }
    }
}
