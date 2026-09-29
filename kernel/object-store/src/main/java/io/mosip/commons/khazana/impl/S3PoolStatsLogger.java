package io.mosip.commons.khazana.impl;

import static io.mosip.commons.khazana.config.LoggerConfiguration.REGISTRATIONID;
import static io.mosip.commons.khazana.config.LoggerConfiguration.SESSIONID;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;

import io.mosip.kernel.core.logger.spi.Logger;

/**
 * AWS SDK v2 {@link MetricPublisher} that samples Apache HTTP client connection-pool metrics
 * ({@link HttpMetric}) at a configurable wall-clock interval while staying on the SDK's public API.
 * <p>
 * Khazana can attach this publisher to an AWS SDK v2 client. Each {@link #publish} call
 * reads leased, available, pending, and max concurrency plus acquire duration, then writes
 * one info line. Lines are throttled to the configured interval. Pending acquires or
 * utilization at or above {@link #HIGH_UTILIZATION_PCT} are logged under a pressure context.
 * Logging failures are swallowed so metric publishing cannot fail the caller.
 */
final class S3PoolStatsLogger implements MetricPublisher {

    /**
     * Utilization percent at or above which a snapshot is logged as pool pressure.
     */
    private static final int HIGH_UTILIZATION_PCT = 90;

    /** Log context for routine pool snapshot lines. */
    private static final String S3_POOL_STATS = "S3PoolStats";

    /** Log context when the pool is under pressure (pending acquires or high utilization). */
    private static final String S3_POOL_STATS_PRESSURE = "S3PoolStatsPressure";

    /**
     * Kernel logger that receives pool snapshot lines.
     */
    private final Logger logger;

    /**
     * Minimum milliseconds between published snapshots. Non-positive constructor input becomes 60 seconds.
     */
    private final long intervalMs;

    /**
     * Epoch milliseconds of the last snapshot that was logged. Used to throttle {@link #publish}.
     */
    private final AtomicLong lastLoggedAtEpochMs = new AtomicLong(0L);

    /**
     * Creates a publisher that logs at most once per interval.
     *
     * @param logger             kernel logger; must accept the Khazana session and registration ids
     * @param logIntervalSeconds seconds between snapshots; values {@code <= 0} use 60 seconds
     */
    S3PoolStatsLogger(Logger logger, int logIntervalSeconds) {
        this.logger = logger;
        int effectiveSeconds = logIntervalSeconds <= 0 ? 60 : logIntervalSeconds;
        this.intervalMs = effectiveSeconds * 1000L;
    }

    /**
     * Samples HTTP pool metrics and logs one snapshot when the interval has elapsed.
     * <p>
     * A null collection is ignored. Concurrent callers lose the race on
     * {@link #lastLoggedAtEpochMs} and return without logging.
     *
     * @param metricCollection SDK metric tree for one request; may be {@code null}
     */
    @Override
    public void publish(MetricCollection metricCollection) {
        if (metricCollection == null) {
            return;
        }
        long now = System.currentTimeMillis();
        long prev = lastLoggedAtEpochMs.get();
        if (now - prev < intervalMs) {
            return;
        }
        if (!lastLoggedAtEpochMs.compareAndSet(prev, now)) {
            return;
        }

        int leased = firstInt(metricCollection, HttpMetric.LEASED_CONCURRENCY, 0);
        int available = firstInt(metricCollection, HttpMetric.AVAILABLE_CONCURRENCY, 0);
        int pending = firstInt(metricCollection, HttpMetric.PENDING_CONCURRENCY_ACQUIRES, 0);
        int max = firstInt(metricCollection, HttpMetric.MAX_CONCURRENCY, 0);
        long acquireMs = firstAcquireMillis(metricCollection);

        int utilizationPct = max > 0 ? leased * 100 / max : 0;
        boolean pressure = pending > 0 || utilizationPct >= HIGH_UTILIZATION_PCT;

        String message = "[S3-POOL] leased=" + leased
                + " pending=" + pending
                + " available=" + available
                + " max=" + max
                + " utilizationPct=" + utilizationPct
                + " acquireMs=" + acquireMs;

        try {
            if (pressure) {
                logger.info(SESSIONID, REGISTRATIONID, S3_POOL_STATS_PRESSURE, message);
            } else {
                logger.info(SESSIONID, REGISTRATIONID, S3_POOL_STATS, message);
            }
        } catch (RuntimeException ignored) {
            // MetricPublisher must not interrupt callers; logging failures are non-fatal.
        }
    }

    /**
     * No-op. This publisher holds no resources of its own.
     */
    @Override
    public void close() {
        // No resources to release.
    }

    /**
     * Returns the first integer value of {@code metric}, or {@code defaultValue} when none is present.
     *
     * @param c            metric tree to search, including children
     * @param metric       integer metric to read
     * @param defaultValue value used when the metric is missing
     * @return first non-null integer, otherwise {@code defaultValue}
     */
    private static int firstInt(MetricCollection c, SdkMetric<Integer> metric, int defaultValue) {
        Integer v = firstIntegerOrNull(c, metric);
        return v != null ? v : defaultValue;
    }

    /**
     * Walks {@code c} and its children for the first non-null integer metric value.
     *
     * @param c      metric tree to search
     * @param metric integer metric to read
     * @return the first value, or {@code null} when the metric is absent
     */
    private static Integer firstIntegerOrNull(MetricCollection c, SdkMetric<Integer> metric) {
        List<Integer> vals = c.metricValues(metric);
        if (vals != null) {
            for (Integer v : vals) {
                if (v != null) {
                    return v;
                }
            }
        }
        for (MetricCollection child : c.children()) {
            Integer v = firstIntegerOrNull(child, metric);
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    /**
     * Returns the concurrency-acquire duration in milliseconds, or {@code 0} when it is absent.
     *
     * @param c metric tree to search
     * @return acquire duration in milliseconds
     */
    private static long firstAcquireMillis(MetricCollection c) {
        Duration d = firstDurationOrNull(c, HttpMetric.CONCURRENCY_ACQUIRE_DURATION);
        return d != null ? d.toMillis() : 0L;
    }

    /**
     * Walks {@code c} and its children for the first non-null duration metric value.
     *
     * @param c      metric tree to search
     * @param metric duration metric to read
     * @return the first duration, or {@code null} when the metric is absent
     */
    private static Duration firstDurationOrNull(MetricCollection c, SdkMetric<Duration> metric) {
        List<Duration> vals = c.metricValues(metric);
        if (vals != null) {
            for (Duration v : vals) {
                if (v != null) {
                    return v;
                }
            }
        }
        for (MetricCollection child : c.children()) {
            Duration v = firstDurationOrNull(child, metric);
            if (v != null) {
                return v;
            }
        }
        return null;
    }
}
