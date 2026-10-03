package de.berlinstream.delay.config;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Environment driven job configuration, validated once at submission time.
 *
 * <p>Every problem found is reported in a single exception. A job that dies on
 * the first bad variable makes the operator resubmit five times to find five
 * typos, and each resubmission costs a cluster round trip.
 */
public final class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String bootstrapServers;
    private final String sourceTopic;
    private final String dlqTopic;
    private final String consumerGroup;
    private final String startingOffsets;

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;

    private final Duration outOfOrderness;
    private final Duration allowedLateness;
    private final Duration idleSourceTimeout;
    private final Duration tumblingSize;
    private final Duration slidingSize;
    private final Duration slidingSlide;

    private final Duration checkpointInterval;
    private final String checkpointDir;
    private final int parallelism;
    private final int sinkBatchSize;
    private final int latencySampleEvery;
    private final int dimensionCacheSize;

    private JobConfig(Builder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.sourceTopic = builder.sourceTopic;
        this.dlqTopic = builder.dlqTopic;
        this.consumerGroup = builder.consumerGroup;
        this.startingOffsets = builder.startingOffsets;
        this.jdbcUrl = builder.jdbcUrl;
        this.jdbcUser = builder.jdbcUser;
        this.jdbcPassword = builder.jdbcPassword;
        this.outOfOrderness = builder.outOfOrderness;
        this.allowedLateness = builder.allowedLateness;
        this.idleSourceTimeout = builder.idleSourceTimeout;
        this.tumblingSize = builder.tumblingSize;
        this.slidingSize = builder.slidingSize;
        this.slidingSlide = builder.slidingSlide;
        this.checkpointInterval = builder.checkpointInterval;
        this.checkpointDir = builder.checkpointDir;
        this.parallelism = builder.parallelism;
        this.sinkBatchSize = builder.sinkBatchSize;
        this.latencySampleEvery = builder.latencySampleEvery;
        this.dimensionCacheSize = builder.dimensionCacheSize;
    }

    /**
     * Reads and validates configuration from an environment map.
     *
     * @param env environment variables, normally {@code System.getenv()}
     * @return a validated configuration
     * @throws IllegalArgumentException if any variable is missing or invalid;
     *     the message lists every problem, not only the first
     */
    public static JobConfig fromEnv(Map<String, String> env) {
        List<String> problems = new ArrayList<>();
        Builder builder = new Builder();

        builder.bootstrapServers = required(env, "KAFKA_BOOTSTRAP_SERVERS", problems);
        builder.sourceTopic = orDefault(env, "TOPIC_STOP_EVENTS", "stop-events");
        builder.dlqTopic = orDefault(env, "TOPIC_DLQ", "stop-events.dlq");
        builder.consumerGroup = orDefault(env, "CONSUMER_GROUP", "delay-pipeline");
        builder.startingOffsets =
                orDefault(env, "STARTING_OFFSETS", "committed").toLowerCase(Locale.ROOT);
        if (!List.of("earliest", "latest", "committed").contains(builder.startingOffsets)) {
            problems.add("STARTING_OFFSETS: must be earliest, latest or committed");
        }

        builder.jdbcUrl = required(env, "JDBC_URL", problems);
        builder.jdbcUser = required(env, "JDBC_USER", problems);
        builder.jdbcPassword = required(env, "JDBC_PASSWORD", problems);

        builder.outOfOrderness = seconds(env, "WATERMARK_OUT_OF_ORDERNESS_S", 300, problems, 0);
        builder.allowedLateness = seconds(env, "ALLOWED_LATENESS_S", 600, problems, 0);
        builder.idleSourceTimeout = seconds(env, "SOURCE_IDLE_TIMEOUT_S", 0, problems, 0);
        builder.tumblingSize = seconds(env, "TUMBLING_WINDOW_S", 3600, problems, 60);
        builder.slidingSize = seconds(env, "SLIDING_WINDOW_S", 3600, problems, 60);
        builder.slidingSlide = seconds(env, "SLIDING_SLIDE_S", 300, problems, 30);
        if (builder.slidingSize.toSeconds() % builder.slidingSlide.toSeconds() != 0) {
            problems.add("SLIDING_WINDOW_S must be an exact multiple of SLIDING_SLIDE_S");
        }

        builder.checkpointInterval = seconds(env, "CHECKPOINT_INTERVAL_S", 30, problems, 1);
        builder.checkpointDir = orDefault(env, "CHECKPOINT_DIR", "file:///flink-checkpoints");
        builder.parallelism = integer(env, "JOB_PARALLELISM", 2, problems, 1, 32);
        builder.sinkBatchSize = integer(env, "SINK_BATCH_SIZE", 500, problems, 1, 50_000);
        builder.latencySampleEvery = integer(env, "LATENCY_SAMPLE_EVERY", 100, problems, 1, 1_000_000);
        builder.dimensionCacheSize = integer(env, "DIMENSION_CACHE_SIZE", 50_000, problems, 100, 1_000_000);

        if (!problems.isEmpty()) {
            throw new IllegalArgumentException("invalid configuration: " + String.join("; ", problems));
        }
        return new JobConfig(builder);
    }

    private static String required(Map<String, String> env, String name, List<String> problems) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            problems.add(name + ": required but not set");
            return "";
        }
        return value.trim();
    }

    private static String orDefault(Map<String, String> env, String name, String fallback) {
        String value = env.get(name);
        return (value == null || value.isBlank()) ? fallback : value.trim();
    }

    private static int integer(
            Map<String, String> env, String name, int fallback, List<String> problems, int min, int max) {
        String raw = env.get(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            int value = Integer.parseInt(raw.trim());
            if (value < min || value > max) {
                problems.add(name + ": " + value + " outside [" + min + ", " + max + "]");
            }
            return value;
        } catch (NumberFormatException ex) {
            problems.add(name + ": '" + raw + "' is not an integer");
            return fallback;
        }
    }

    private static Duration seconds(
            Map<String, String> env, String name, int fallback, List<String> problems, int min) {
        return Duration.ofSeconds(integer(env, name, fallback, problems, min, Integer.MAX_VALUE));
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    public String sourceTopic() {
        return sourceTopic;
    }

    public String dlqTopic() {
        return dlqTopic;
    }

    public String consumerGroup() {
        return consumerGroup;
    }

    public String startingOffsets() {
        return startingOffsets;
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public String jdbcUser() {
        return jdbcUser;
    }

    public String jdbcPassword() {
        return jdbcPassword;
    }

    public Duration outOfOrderness() {
        return outOfOrderness;
    }

    public Duration allowedLateness() {
        return allowedLateness;
    }

    public Duration idleSourceTimeout() {
        return idleSourceTimeout;
    }

    public Duration tumblingSize() {
        return tumblingSize;
    }

    public Duration slidingSize() {
        return slidingSize;
    }

    public Duration slidingSlide() {
        return slidingSlide;
    }

    public Duration checkpointInterval() {
        return checkpointInterval;
    }

    public String checkpointDir() {
        return checkpointDir;
    }

    public int parallelism() {
        return parallelism;
    }

    public int sinkBatchSize() {
        return sinkBatchSize;
    }

    public int latencySampleEvery() {
        return latencySampleEvery;
    }

    public int dimensionCacheSize() {
        return dimensionCacheSize;
    }

    private static final class Builder {
        private String bootstrapServers;
        private String sourceTopic;
        private String dlqTopic;
        private String consumerGroup;
        private String startingOffsets;
        private String jdbcUrl;
        private String jdbcUser;
        private String jdbcPassword;
        private Duration outOfOrderness;
        private Duration allowedLateness;
        private Duration idleSourceTimeout;
        private Duration tumblingSize;
        private Duration slidingSize;
        private Duration slidingSlide;
        private Duration checkpointInterval;
        private String checkpointDir;
        private int parallelism;
        private int sinkBatchSize;
        private int latencySampleEvery;
        private int dimensionCacheSize;
    }
}
