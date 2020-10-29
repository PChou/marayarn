package com.eoi.marayarn.logstash;

import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.HandlerErrorException;
import com.eoi.marayarn.http.JsonUtil;
import com.eoi.marayarn.http.ProcessResult;
import com.eoi.marayarn.prometheus.protobuf.Types;
import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.handler.codec.http.HttpMethod;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MonitoringHandler implements Handler {

    public static final String METRIC_NAME_KEY = "__name__";

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
                        long timestamp = System.currentTimeMillis();
                        LogstashStatsMetricsPicker picker = new LogstashStatsMetricsPicker(parsed);
                        picker.visit();
                        List<Types.TimeSeries> ts = toProm(picker.metricValues, picker.metricTypes, timestamp,
                                this.applicationMaster.applicationAttemptId.getApplicationId().toString());
                        applicationMaster.writeMetricsToInfluxdb(ts);
                    }
                }
                line = bufferedReader.readLine();
            }
        } catch (Exception e) {
            return ProcessResult.jsonProcessResult(BulkAck.FAILED(1));
        }
        return ProcessResult.jsonProcessResult(BulkAck.OK(1));
    }

    private List<Types.TimeSeries> toProm(Map<String, Object> values, Map<String, String> types, long timestamp, String yarnId) {
        List<Types.TimeSeries> timeSeries = new ArrayList<>();
        for (String metricKey: values.keySet()) {
            Object value = values.get(metricKey);
            // String type = types.get(metricKey);
            if (value == null) {
                continue;
            }
            List<Types.Label> commonLabel = new ArrayList<>(8);
            commonLabel.add(Types.Label.newBuilder().setName("job").setValue(yarnId).build());
            commonLabel.add(Types.Label.newBuilder().setName(METRIC_NAME_KEY).setValue("logstash" + metricKey).build());
            Types.TimeSeries ts = new Types.TimeSeries();
            Types.Sample.Builder builder =  Types.Sample.newBuilder().setTimestamp(timestamp);
            builder.setValue(Double.parseDouble(value.toString()));
            ts.setSamples(Collections.singletonList(builder.build()));
            ts.setLabels(commonLabel);
            timeSeries.add(ts);
        }
        return timeSeries;
    }
}
