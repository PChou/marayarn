package com.eoi.marayarn.prometheus;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;

public class VictoriaMetricsReporter extends PrometheusMetricReporter {

    protected static final Logger log = LoggerFactory.getLogger(VictoriaMetricsReporter.class);

    @Override
    public void start() {
        return;
    }

    @Override
    public void stop() {
        return;
    }

    /**
     * https://docs.victoriametrics.com/Single-server-VictoriaMetrics.html#how-to-import-data-in-prometheus-exposition-format
     * use prometheus text format
     * foo{lable1="",lable2=""} value
     */
    @Override
    public void flush() {
        String url = getEndpoint();
        if (!getEndpoint().endsWith("/api/v1/import/prometheus")) {
            url = url + "/api/v1/import/prometheus";
        }
        Enumeration<MetricFamilySamples> mfs = CollectorRegistry.defaultRegistry.metricFamilySamples();
        StringBuilder cb = new StringBuilder();
        while(mfs.hasMoreElements()) {
            MetricFamilySamples metricFamilySamples = mfs.nextElement();
            for (Sample sample : metricFamilySamples.samples) {
                cb.append(sample.name);
                if (sample.labelNames.size() > 0) {
                    cb.append("{");
                    for(int i = 0; i < sample.labelNames.size(); ++i) {
                        cb.append(sample.labelNames.get(i));
                        cb.append("=\"");
                        cb.append(sample.labelValues.get(i));
                        cb.append("\",");
                    }
                    cb.append("}");
                }
                cb.append(" ");
                cb.append(Collector.doubleToGoString(sample.value));
                cb.append("\n");
            }
        }
        try {
            doPost(url, cb.toString());
        } catch (Exception e) {
            log.warn(
                    "Failed to push metrics to VictoriaMetrics with jobName {}",
                    getJobName(),
                    e);
        }
    }

    private void doPost(String url, String body) throws Exception {
        HttpURLConnection connection = (HttpURLConnection)(new URL(url)).openConnection();
        connection.setRequestProperty("Content-Type", "text/plain;");
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(10000);
        connection.connect();
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));
            writer.write(body);
            writer.flush();
            writer.close();
            int response = connection.getResponseCode();
            if (response / 100 != 2) {
                InputStream errorStream = connection.getErrorStream();
                String errorMessage;
                if (errorStream != null) {
                    String errBody = readFromStream(errorStream);
                    errorMessage = "Response code from " + url + " was " + response + ", response body: " + errBody;
                } else {
                    errorMessage = "Response code from " + url + " was " + response;
                }

                throw new IOException(errorMessage);
            }
        } finally {
            connection.disconnect();
        }
    }

    private static String readFromStream(InputStream is) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];

        int length;
        while((length = is.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }

        return result.toString("UTF-8");
    }
}
