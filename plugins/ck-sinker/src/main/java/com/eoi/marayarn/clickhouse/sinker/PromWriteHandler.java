package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.MaraApplicationMaster;
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
        return new HashMap<>();
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
                    toProm(metric, this.applicationMaster.getJobId());
                }
                metricFamily = dataParser.parse();
            }
            this.applicationMaster.prometheusMetricReporter.flush();
            return ProcessResult.jsonProcessResult(AckResponse.OK);
        } catch (Exception ex) {
            logger.warn("Failed to parse clickhouse metrics into prometheus metrics", ex);
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
        }
    }

    private void toProm(Metric metric, String yarnId) {
        Map<String, String> labels = new HashMap<>();
        labels.put("application", yarnId);
        labels.putAll(metric.getLabels());
        if (metric instanceof Counter) {
            applicationMaster.prometheusMetricReporter
                    .putFullCounter(metric.getName(), labels, ((Counter) metric).getValue());
        } else if (metric instanceof Gauge) {
            applicationMaster.prometheusMetricReporter
                    .putGauge(metric.getName(), labels, ((Gauge) metric).getValue());
        }
    }
}
