package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.clickhouse.sinker.parse.text.TextPrometheusMetricDataParser;
import com.eoi.marayarn.clickhouse.sinker.parse.types.*;
import com.eoi.marayarn.prometheus.protobuf.Types;
import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.HandlerErrorException;
import com.eoi.marayarn.http.ProcessResult;
import com.eoi.marayarn.http.model.AckResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.ByteArrayInputStream;
import java.util.*;

public class PromWriteHandler implements Handler {
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
            long timestamp = System.currentTimeMillis();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body);
            // parse
            TextPrometheusMetricDataParser dataParser = new TextPrometheusMetricDataParser(byteArrayInputStream);
            MetricFamily metricFamily = dataParser.parse();
            List<Types.TimeSeries> tsList = new ArrayList<>(64);
            while (metricFamily != null) {
                for (Metric metric: metricFamily.getMetrics()) {
                    PromConverter.convertAndFill(tsList, metric, timestamp, this.applicationMaster.getJobId());
                }
                metricFamily = dataParser.parse();
            }
            applicationMaster.writeMetricsToInfluxdb(tsList);
            return ProcessResult.jsonProcessResult(AckResponse.OK);
        } catch (Exception ex) {
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
        }
    }
}
