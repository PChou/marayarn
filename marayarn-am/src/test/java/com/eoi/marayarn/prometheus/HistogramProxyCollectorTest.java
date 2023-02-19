package com.eoi.marayarn.prometheus;

import com.eoi.marayarn.MetricsReporter.Bucket;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class HistogramProxyCollectorTest {

    @Test
    public void collectTest() {
        HistogramProxyCollector collector =
                HistogramProxyCollector.build("clickhouse_sinker_writing_durations", "clickhouse_sinker_writing_durations writing durations")
                        .labelNames("table", "task").create();
        collector.labels("test_r3", "test_r3")
                .update(
                        Arrays.asList(
                                new Bucket(1.0, 52),
                                new Bucket(2.0, 184),
                                new Bucket(4.0, 188),
                                new Bucket(8.0, 190),
                                new Bucket(16.0, 190),
                                new Bucket(128.0, 190),
                                new Bucket(Double.POSITIVE_INFINITY, 190)
                        ),
                247.7,
                190);

        collector.labels("test_r3", "test_r3")
                .update(
                        Arrays.asList(
                                new Bucket(2.0, 184),
                                new Bucket(4.0, 188),
                                new Bucket(8.0, 190),
                                new Bucket(16.0, 190),
                                new Bucket(128.0, 190),
                                new Bucket(Double.POSITIVE_INFINITY, 190)
                        ),
                        247.7,
                        190);

        collector.labels("test_r2", "test_r2")
                .update(
                        Arrays.asList(
                                new Bucket(1.0, 52),
                                new Bucket(2.0, 184),
                                new Bucket(4.0, 188),
                                new Bucket(8.0, 190),
                                new Bucket(16.0, 190),
                                new Bucket(128.0, 190),
                                new Bucket(Double.POSITIVE_INFINITY, 190)
                        ),
                        247.7,
                        190);

        List<MetricFamilySamples> metricFamilySamplesList = collector.collect();
        Assert.assertEquals(3, metricFamilySamplesList.size());
    }
}