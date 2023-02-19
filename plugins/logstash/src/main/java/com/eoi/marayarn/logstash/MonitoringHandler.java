package com.eoi.marayarn.logstash;

import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.HandlerErrorException;
import com.eoi.marayarn.http.JsonUtil;
import com.eoi.marayarn.http.ProcessResult;
import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MonitoringHandler implements Handler {

    private static final Logger logger = LoggerFactory.getLogger(MonitoringHandler.class);

    private MaraApplicationMaster applicationMaster;
    public MonitoringHandler(MaraApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public Map<String, String> match(String uri, HttpMethod method) {
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        Map<String, String> uriParams = new HashMap<>();
        if (uri.startsWith("/logstash/_monitoring/bulk") && method.equals(HttpMethod.POST)) {
            uriParams.put("monitoring", "true");
        } else {
            return null;
        }
        return uriParams;
    }

    @Override
    public ProcessResult process(Map<String, String> urlParams, HttpMethod method, byte[] body)
            throws HandlerErrorException {
        try {
            StringReader stringReader = new StringReader(new String(body, StandardCharsets.UTF_8));
            BufferedReader bufferedReader = new BufferedReader(stringReader);
            String line = bufferedReader.readLine();
            String lastType = null;
            while(line != null) {
                Map<String, Object> parsed =
                        JsonUtil._mapper.readValue(line, new TypeReference<Map<String, Object>>() { });
                if (parsed.containsKey("index")) {
                    lastType = ((Map)parsed.get("index")).get("_type").toString();
                } else {
                    if (lastType != null && lastType.equals("logstash_stats")) {
                        LogstashStatsMetricsPicker picker = new LogstashStatsMetricsPicker(parsed);
                        picker.visit();
                        toProm(picker.uuid, picker.metricValues, picker.metricTypes, this.applicationMaster.getJobId());
                    }
                }
                line = bufferedReader.readLine();
            }
        } catch (Exception e) {
            logger.warn("Failed to parse logstash metrics into prometheus metrics", e);
            return ProcessResult.jsonProcessResult(BulkAck.FAILED(1));
        }
        return ProcessResult.jsonProcessResult(BulkAck.OK(1));
    }

    private void toProm(String uuid, Map<String, Object> values, Map<String, String> types, String yarnId) {
        for (String metricKey: values.keySet()) {
            Object value = values.get(metricKey);
            String type = types.get(metricKey);
            if (value == null) {
                continue;
            }
            Map<String, String> labels = new HashMap<>();
            labels.put("application", yarnId);
            labels.put("uuid", uuid);
            if ("gauge".equals(type)) {
                applicationMaster.metricsReporter.putGauge("logstash" + metricKey,
                        labels, Double.parseDouble(value.toString()));
            } else if ("counter".equals(type)) {
                applicationMaster.metricsReporter.putFullCounter("logstash" + metricKey,
                        labels, Double.parseDouble(value.toString()));
            }
        }
        applicationMaster.metricsReporter.flush();
    }
}
