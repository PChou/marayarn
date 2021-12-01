package com.eoi.marayarn.logstash;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class LogstashStatsMetricsPicker extends JsonVisitor {
    private static final Logger logger = LoggerFactory.getLogger(LogstashStatsMetricsPicker.class);

    private static Map<String, BiConsumer<LogstashStatsMetricsPicker, Object>> valuePicker = new HashMap<>();

    static {
        valuePicker.put(".logstash.uuid", (picker, v) -> picker.uuid = v.toString());
    }

    public Map<String, Object> metricValues = new HashMap<>();
    public Map<String, String> metricTypes = new HashMap<>();
    public String uuid;

    public LogstashStatsMetricsPicker(String json) {
        super(json);
        init();
    }

    public LogstashStatsMetricsPicker(Map<String, Object> parsedMap) {
        super(parsedMap);
        init();
    }

    private void init() {
        try {
            InputStream exportedMetrics = getClass().getResourceAsStream("/exported_logstash_stats_metrics");
            byte[] metrics = IOUtils.toByteArray(exportedMetrics);
            StringReader stringReader = new StringReader(new String(metrics));
            BufferedReader bufferedReader = new BufferedReader(stringReader);
            String line = bufferedReader.readLine();
            while(line != null) {
                String[] metricLine = line.split(",");
                metricTypes.put(metricLine[0], metricLine[1]);
                line = bufferedReader.readLine();
            }
        } catch (IOException ioException) {
            logger.warn("Failed to init LogstashStatsMetricsPicker", ioException);
        }
    }

    @Override
    protected boolean internalVisit(String path, Object value) {

        for (String valueKey: valuePicker.keySet()) {
            if (!valueKey.startsWith(path)) continue;
            if (valueKey.equals(path)) {
                valuePicker.get(valueKey).accept(this, value);
            }
            return true;
        }

        for (String metricKey: metricTypes.keySet()) {
            // metricKey不包含path开头的话，看下一个
            if (!metricKey.startsWith(path)) continue;
            // 包含的话，如果完全相同就pick到值了
            if (metricKey.equals(path)) {
                metricValues.put(path, value);
            }
            // 到这里肯定是包含了
            return true;
        }
        // 如果所有的metricKey都continue了，说明这个path不关心，返回false，阻止向下进一步遍历
        return false;
    }
}
