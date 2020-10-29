package com.eoi.marayarn.logstash;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class LogstashStatsMetricsPickerTest {

    @Test
    public void LogstashStatsMetricsPickerTest_1() throws Exception {
        byte[] metrics = IOUtils.toByteArray(getClass().getResourceAsStream("/test.json"));
        LogstashStatsMetricsPicker logstashStatsMetricsPicker = new LogstashStatsMetricsPicker(new String(metrics));
        logstashStatsMetricsPicker.visit();
        Assert.assertEquals(1, logstashStatsMetricsPicker.metricValues.get(".process.cpu.percent"));
        Assert.assertFalse(logstashStatsMetricsPicker.metricValues.containsKey(".reloads.failures"));
        Assert.assertEquals(14, logstashStatsMetricsPicker.metricValues.size());
        Assert.assertEquals(14, logstashStatsMetricsPicker.metricTypes.size());
    }
}