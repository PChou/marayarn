package com.eoi.marayarn.test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PromPGWRepoter {
    public static void main(String[] args) {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(() -> {
            try {
                String endpoint = System.getenv("METRIC_PUSH_GATEWAY_ADDRS");
                if (null == endpoint || endpoint.isEmpty()) {
                    System.out.println("Invalid METRIC_PUSH_GATEWAY_ADDRS, skip reporter metrics");
                    return;
                }
                URL url = new URL(endpoint);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("PUT");
                //指标数据
                String postData = genData();
                byte[] postDataBytes = postData.getBytes();
                con.setDoOutput(true);
                con.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length));
                con.getOutputStream().write(postDataBytes);
                con.getOutputStream().flush();
                con.getOutputStream().close();
                System.out.println("Response code:" + con.getResponseCode());
            } catch (Exception e) {
                System.out.println("Exception:" + e.getMessage());
            }
        }, 0, 15, TimeUnit.SECONDS);
    }

    public static String genData() {
        List<String> metrics = new ArrayList<>();
        // gauge
        metrics.add("# TYPE prom_test_reporter_gauge gauge");
        metrics.add("prom_test_reporter_gauge{uniq_labels=\"label1\"} 13.37");

        metrics.add("# TYPE prom_test_reporter_counter counter");
        metrics.add("prom_test_reporter_counter{uniq_labels=\"label1\"} 1");

        metrics.add("# TYPE prom_test_reporter_histogram histogram");
        metrics.add("prom_test_reporter_histogram_bucket{uniq_labels=\"label1\",le=\"1\"} 1");
        metrics.add("prom_test_reporter_histogram_bucket{uniq_labels=\"label1\",le=\"2\"} 2");
        metrics.add("prom_test_reporter_histogram_bucket{uniq_labels=\"label1\",le=\"+Inf\"} 10");
        metrics.add("prom_test_reporter_histogram_count{uniq_labels=\"label1\"} 10");
        metrics.add("prom_test_reporter_histogram_sum{uniq_labels=\"label1\"} 247.76805632999998");

        metrics.add("# TYPE go_gc_duration_seconds summary");
        metrics.add("go_gc_duration_seconds{quantile=\"0\"} 4.5641e-05");
        metrics.add("go_gc_duration_seconds{quantile=\"0.25\"} 6.0903e-05");
        metrics.add("go_gc_duration_seconds{quantile=\"0.5\"} 0.000114559");
        metrics.add("go_gc_duration_seconds{quantile=\"0.75\"} 0.00014669");
        metrics.add("go_gc_duration_seconds{quantile=\"1\"} 0.00046615");
        metrics.add("go_gc_duration_seconds_sum 0.003350916");
        metrics.add("go_gc_duration_seconds_count 27");

        return String.join("\n", metrics);
    }
}
