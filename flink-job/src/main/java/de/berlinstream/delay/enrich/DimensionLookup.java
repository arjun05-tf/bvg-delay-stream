package de.berlinstream.delay.enrich;

import de.berlinstream.delay.config.JobConfig;
import de.berlinstream.delay.model.EnrichedEvent;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves human readable route and stop names from the GTFS dimension tables.
 *
 * <p>A cached lookup join rather than a broadcast state join. The two
 * dimension tables hold on the order of tens of thousands of rows, the cache
 * reaches a hit rate above 99 percent within a minute of traffic, and a miss
 * costs one indexed primary key lookup. Broadcasting the tables would be
 * faster still and would also pin them in managed memory on a task manager
 * that has very little of it.
 *
 * <p>A miss that returns nothing is not an error. The VBB realtime feed
 * regularly references stop ids that the current schedule export does not
 * contain, so the event keeps its id as its name and a counter records it.
 */
public class DimensionLookup extends RichMapFunction<EnrichedEvent, EnrichedEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DimensionLookup.class);

    private static final String STOP_QUERY = "SELECT stop_name FROM gtfs_stops WHERE stop_id = ?";
    private static final String ROUTE_QUERY =
            "SELECT route_short_name FROM gtfs_routes WHERE route_id = ?";

    private final JobConfig config;

    private transient Connection connection;
    private transient PreparedStatement stopStatement;
    private transient PreparedStatement routeStatement;
    private transient Map<String, String> stopCache;
    private transient Map<String, String> routeCache;
    private transient Counter cacheHits;
    private transient Counter cacheMisses;
    private transient Counter unknownDimensions;

    public DimensionLookup(JobConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws SQLException {
        connection = DriverManager.getConnection(
                config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword());
        connection.setReadOnly(true);
        stopStatement = connection.prepareStatement(STOP_QUERY);
        routeStatement = connection.prepareStatement(ROUTE_QUERY);
        stopCache = newCache(config.dimensionCacheSize());
        routeCache = newCache(config.dimensionCacheSize());
        cacheHits = getRuntimeContext().getMetricGroup().counter("dimensionCacheHits");
        cacheMisses = getRuntimeContext().getMetricGroup().counter("dimensionCacheMisses");
        unknownDimensions = getRuntimeContext().getMetricGroup().counter("dimensionNotFound");
    }

    /** Returns a fixed size LRU map. */
    private static Map<String, String> newCache(int capacity) {
        return new LinkedHashMap<>(Math.min(capacity, 1 << 14), 0.75f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > capacity;
            }
        };
    }

    @Override
    public EnrichedEvent map(EnrichedEvent event) throws SQLException {
        event.stopName = lookup(stopCache, stopStatement, event.stopId);
        event.routeShortName = lookup(routeCache, routeStatement, event.routeId);
        return event;
    }

    private String lookup(Map<String, String> cache, PreparedStatement statement, String key)
            throws SQLException {
        String cached = cache.get(key);
        if (cached != null) {
            cacheHits.inc();
            return cached;
        }
        cacheMisses.inc();
        statement.setString(1, key);
        String value = key;
        try (ResultSet rows = statement.executeQuery()) {
            if (rows.next()) {
                value = rows.getString(1);
            } else {
                unknownDimensions.inc();
            }
        }
        // Negative results are cached too, under the key itself. Without that,
        // every event for an unknown stop would hit the database forever.
        cache.put(key, value);
        return value;
    }

    @Override
    public void close() {
        closeQuietly(stopStatement);
        closeQuietly(routeStatement);
        closeQuietly(connection);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ex) {
            LOG.warn("failed to close JDBC resource", ex);
        }
    }
}
