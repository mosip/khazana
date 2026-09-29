package io.mosip.commons.khazana.impl;

import io.mosip.kernel.core.logger.spi.Logger;
import org.junit.Test;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3PoolStatsLoggerTest {

    @Test
    public void publishPressureIdleAndChildMetrics() {
        Logger logger = mock(Logger.class);
        S3PoolStatsLogger publisher = new S3PoolStatsLogger(logger, 0);
        publisher.publish(null);
        publisher.publish(collection(9, 1, 1, 10, Duration.ofMillis(4), List.of()));
        publisher.publish(collection(1, 9, 0, 10, null, List.of()));
        publisher.close();

        MetricCollection parent = mock(MetricCollection.class);
        when(parent.metricValues(org.mockito.ArgumentMatchers.any())).thenReturn(null);
        MetricCollection child = collection(1, 1, 0, 2, Duration.ofMillis(1), List.of());
        when(parent.children()).thenReturn(List.of(child));
        S3PoolStatsLogger immediate = new S3PoolStatsLogger(logger, 1);
        Reflection.setInterval(immediate);
        immediate.publish(parent);

        doThrow(new RuntimeException("log")).when(logger).info(anyString(), anyString(), anyString(), anyString());
        new S3PoolStatsLogger(logger, 1).publish(collection(1, 1, 0, 2, Duration.ofMillis(1), List.of()));
    }

    private static MetricCollection collection(int leased, int available, int pending, int max, Duration acquire,
                                               List<MetricCollection> children) {
        MetricCollection c = mock(MetricCollection.class);
        when(c.metricValues(HttpMetric.LEASED_CONCURRENCY)).thenReturn(List.of(leased));
        when(c.metricValues(HttpMetric.AVAILABLE_CONCURRENCY)).thenReturn(List.of(available));
        when(c.metricValues(HttpMetric.PENDING_CONCURRENCY_ACQUIRES)).thenReturn(java.util.Arrays.asList(pending, null));
        when(c.metricValues(HttpMetric.MAX_CONCURRENCY)).thenReturn(List.of(max));
        if (acquire == null) {
            when(c.metricValues(HttpMetric.CONCURRENCY_ACQUIRE_DURATION)).thenReturn(List.of());
        } else {
            when(c.metricValues(HttpMetric.CONCURRENCY_ACQUIRE_DURATION)).thenReturn(List.of(acquire));
        }
        when(c.children()).thenReturn(children);
        return c;
    }

    /** Test hook: force the sample window open without waiting. */
    static final class Reflection {
        static void setInterval(S3PoolStatsLogger logger) {
            org.springframework.test.util.ReflectionTestUtils.setField(logger, "intervalMs", 0L);
            org.springframework.test.util.ReflectionTestUtils.setField(logger, "lastLoggedAtEpochMs", new java.util.concurrent.atomic.AtomicLong(0L));
        }
    }
}
