package com.eoi.marayarn.test;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PromPGWRepoter {

    public static void main(String[] args) {
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

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(() -> {
            String endpoint = System.getenv("METRIC_PUSH_GATEWAY_ADDRS");
            if (null == endpoint || endpoint.isEmpty()) {
                System.out.println("Invalid METRIC_PUSH_GATEWAY_ADDRS, skip reporter metrics");
                return;
            }
            URL url = null;
            HttpURLConnection con = null;
            try {
                url = new URL(endpoint);
                con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("PUT");
                con.setDoOutput(true);
                //指标数据
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(con.getOutputStream(), "UTF-8"));
                genData(writer, counter, gauge, histogram, summary);
                writer.flush();
                writer.close();
                System.out.println("Response code:" + con.getResponseCode());
            } catch (Exception e) {
                System.out.println("Exception:" + e.getMessage());
            } finally {
                if (con != null) {
                    con.disconnect();
                }
            }
        }, 0, 15, TimeUnit.SECONDS);
    }

    public static void genData(Writer writer, Counter counter, Gauge gauge, Histogram histogram, Summary summary)
        throws Exception {
        counter.labels("task1").inc(new Random().nextInt(30));
        gauge.labels("task1").set(new Random().nextInt(500));
        histogram.labels("task1").observe(new Random().nextInt(801));
        summary.labels("task1").observe(new Random().nextInt(10000));
        TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
    }
}
