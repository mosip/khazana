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
 */
final class S3PoolStatsLogger implements MetricPublisher {

    private static final int HIGH_UTILIZATION_PCT = 90;

    /** Log context for routine pool snapshot lines. */
    private static final String S3_POOL_STATS = "S3PoolStats";

    /** Log context when the pool is under pressure (pending acquires or high utilization). */
    private static final String S3_POOL_STATS_PRESSURE = "S3PoolStatsPressure";

    private final Logger logger;
    private final long intervalMs;
    private final AtomicLong lastLoggedAtEpochMs = new AtomicLong(0L);

    S3PoolStatsLogger(Logger logger, int logIntervalSeconds) {
        this.logger = logger;
        int effectiveSeconds = logIntervalSeconds <= 0 ? 60 : logIntervalSeconds;
        this.intervalMs = effectiveSeconds * 1000L;
    }

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

    @Override
    public void close() {
        // No resources to release.
    }

    private static int firstInt(MetricCollection c, SdkMetric<Integer> metric, int defaultValue) {
        Integer v = firstIntegerOrNull(c, metric);
        return v != null ? v : defaultValue;
    }

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

    private static long firstAcquireMillis(MetricCollection c) {
        Duration d = firstDurationOrNull(c, HttpMetric.CONCURRENCY_ACQUIRE_DURATION);
        return d != null ? d.toMillis() : 0L;
    }

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
