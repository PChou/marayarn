package com.eoi.marayarn.test;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import org.junit.Test;

import java.io.StringWriter;

import static org.junit.Assert.*;

public class PromPGWRepoterTest {

    @Test
    public void genData() throws Exception {

        // 注册指标
        final Counter counter = Counter.build("prom_test_reporter_counter", "prom_test_reporter_counter")
                .labelNames("task")
                .create();
        final Gauge gauge = Gauge.build("prom_test_reporter_gauge", "prom_test_reporter_gauge")
                .labelNames("task")
                .create();
        final Histogram histogram = Histogram
                .build("prom_test_reporter_histogram", "prom_test_reporter_histogram")
                .labelNames("task")
                .buckets(10.0, 50.0, 100.0, 400.0, 800.0)
                .create();
        final Summary summary = Summary.build("prom_test_reporter_summary", "prom_test_reporter_summary")
                .labelNames("task")
                .quantile(0.25, 0.01)
                .quantile(0.5, 0.01)
                .quantile(0.75, 0.005)
                .quantile(1.0, 0.005)
                .create();

        CollectorRegistry.defaultRegistry.register(counter);
        CollectorRegistry.defaultRegistry.register(gauge);
        CollectorRegistry.defaultRegistry.register(histogram);
        CollectorRegistry.defaultRegistry.register(summary);

        StringWriter sw = new StringWriter();
        PromPGWRepoter.genData(sw, counter, gauge, histogram, summary);
        System.out.println(sw.toString());
    }
}