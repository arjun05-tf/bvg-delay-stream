package de.berlinstream.delay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import de.berlinstream.delay.config.JobConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Configuration validation. */
class JobConfigTest {

    private static Map<String, String> minimal() {
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092");
        env.put("JDBC_URL", "jdbc:postgresql://timescaledb:5432/delays");
        env.put("JDBC_USER", "delays");
        env.put("JDBC_PASSWORD", "secret");
        return env;
    }

    @Test
    void appliesDefaults() {
        JobConfig config = JobConfig.fromEnv(minimal());

        assertEquals("stop-events", config.sourceTopic());
        assertEquals(3600, config.tumblingSize().toSeconds());
        assertEquals(300, config.slidingSlide().toSeconds());
        assertEquals(600, config.allowedLateness().toSeconds());
        assertTrue(config.idleSourceTimeout().isZero(), "idleness must be off by default");
    }

    @Test
    void reportsEveryMissingVariableAtOnce() {
        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> JobConfig.fromEnv(Map.of()));

        assertTrue(error.getMessage().contains("KAFKA_BOOTSTRAP_SERVERS"));
        assertTrue(error.getMessage().contains("JDBC_URL"));
        assertTrue(error.getMessage().contains("JDBC_PASSWORD"));
    }

    @Test
    void rejectsSlideThatDoesNotDivideTheWindow() {
        Map<String, String> env = minimal();
        env.put("SLIDING_WINDOW_S", "3600");
        env.put("SLIDING_SLIDE_S", "700");

        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> JobConfig.fromEnv(env));

        assertTrue(error.getMessage().contains("exact multiple"));
    }

    @Test
    void rejectsUnknownStartingOffsets() {
        Map<String, String> env = minimal();
        env.put("STARTING_OFFSETS", "yesterday");

        assertThrows(IllegalArgumentException.class, () -> JobConfig.fromEnv(env));
    }

    @Test
    void rejectsNonNumericParallelism() {
        Map<String, String> env = minimal();
        env.put("JOB_PARALLELISM", "lots");

        IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> JobConfig.fromEnv(env));

        assertTrue(error.getMessage().contains("JOB_PARALLELISM"));
    }
}
