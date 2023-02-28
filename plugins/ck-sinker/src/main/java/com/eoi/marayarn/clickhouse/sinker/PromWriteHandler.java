package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.MetricsReporter;
import com.eoi.marayarn.clickhouse.sinker.parse.text.TextPrometheusMetricDataParser;
import com.eoi.marayarn.clickhouse.sinker.parse.types.*;
import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.HandlerErrorException;
import com.eoi.marayarn.http.ProcessResult;
import com.eoi.marayarn.http.model.AckResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.stream.Collectors;

public class PromWriteHandler implements Handler {
    private static final Logger logger = LoggerFactory.getLogger(PromWriteHandler.class);

    private MaraApplicationMaster applicationMaster;

    public PromWriteHandler(MaraApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public Map<String, String> match(String uri, HttpMethod method) {
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        if (!uri.startsWith("/cks/prom/write") || !method.equals(HttpMethod.PUT) ) {
            return null;
        }
        String[] segment = uri.split("/");
        if (segment.length < 5) {
            return new HashMap<>();
        } else {
            Map<String, String> params = new HashMap<>();
            params.put("containerId", segment[4]);
            return params;
        }
    }

    @Override
    public ProcessResult process(Map<String, String> urlParams, HttpMethod method, byte[] body)
            throws HandlerErrorException {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body);
            // parse
            TextPrometheusMetricDataParser dataParser = new TextPrometheusMetricDataParser(byteArrayInputStream);
            MetricFamily metricFamily = dataParser.parse();
            while (metricFamily != null) {
                for (Metric metric: metricFamily.getMetrics()) {
                    toProm(metric, this.applicationMaster.getJobId(), urlParams);
                }
                metricFamily = dataParser.parse();
            }
            this.applicationMaster.metricsReporter.flush();
            return ProcessResult.jsonProcessResult(AckResponse.OK);
        } catch (Exception ex) {
            logger.warn("Failed to parse clickhouse metrics into prometheus metrics", ex);
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
        }
    }

    private void toProm(Metric metric, String yarnId, Map<String, String> urlParams) {
        Map<String, String> labels = new HashMap<>();
        // inject application_id and container_id
        labels.put("application_id", yarnId);
        if (urlParams.containsKey("containerId")) {
            labels.put("container_id", urlParams.get("containerId"));
        }
        labels.putAll(metric.getLabels());
        if (metric instanceof Counter) {
            applicationMaster.metricsReporter
                    .putFullCounter(metric.getName(), labels, ((Counter) metric).getValue());
        } else if (metric instanceof Gauge) {
            applicationMaster.metricsReporter
                    .putGauge(metric.getName(), labels, ((Gauge) metric).getValue());
        } else if (metric instanceof Histogram) {
            Histogram histogram = (Histogram) metric;
            List<MetricsReporter.Bucket> buckets =  histogram.getBuckets().stream().map(bucket ->
                    new MetricsReporter.Bucket(bucket.getUpperBound(), bucket.getCumulativeCount()))
                    .collect(Collectors.toList());
            applicationMaster.metricsReporter
                    .putHistogram(histogram.getName(), labels, buckets, histogram.getSampleSum(), histogram.getSampleCount());
        } else if (metric instanceof  Summary) {
            Summary summary = (Summary) metric;
            List<MetricsReporter.Quantile> buckets =  summary.getQuantiles().stream().map(bucket ->
                    new MetricsReporter.Quantile(bucket.getQuantile(), bucket.getValue()))
                    .collect(Collectors.toList());
            applicationMaster.metricsReporter
                    .putSummary(summary.getName(), labels, buckets, summary.getSampleSum(), summary.getSampleCount());
        }
    }
}
