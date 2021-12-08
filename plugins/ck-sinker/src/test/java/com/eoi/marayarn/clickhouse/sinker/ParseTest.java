package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.clickhouse.sinker.parse.text.TextPrometheusMetricDataParser;
import com.eoi.marayarn.clickhouse.sinker.parse.types.Histogram;
import com.eoi.marayarn.clickhouse.sinker.parse.types.Metric;
import com.eoi.marayarn.clickhouse.sinker.parse.types.MetricFamily;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

public class ParseTest {
    @Test
    public void parseTest() throws Exception {
        InputStream inputStream = getClass().getResourceAsStream("/testMetrics");
        TextPrometheusMetricDataParser parser = new TextPrometheusMetricDataParser(inputStream);
        MetricFamily metricFamily = parser.parse();
        int metricCount = 0;
        while(metricFamily != null) {
            System.out.println("--------------");
            List<Metric> metricList = metricFamily.getMetrics();
            for (Metric metric: metricList) {
                metricCount++;
                System.out.println(metric.getName());
            }
            metricFamily = parser.parse();
        }
        Assert.assertEquals(48, metricCount);
    }

    @Test
    public void parseTest2() throws Exception {
        InputStream inputStream = getClass().getResourceAsStream("/prometheusMetrics");
        TextPrometheusMetricDataParser parser = new TextPrometheusMetricDataParser(inputStream);
        MetricFamily metricFamily = parser.parse();
        int metricCount = 0;
        while (metricFamily != null) {
            System.out.println("--------------");
            List<Metric> metricList = metricFamily.getMetrics();
            for (Metric metric : metricList) {
                metricCount++;
                if (metric instanceof Histogram) {
                    System.out.println(metric.getName());
                    List<Histogram.Bucket> buckets = ((Histogram) metric).getBuckets();
                    for (Histogram.Bucket bucket: buckets) {
                        System.out.println(bucket.getUpperBound());
                    }
                }
            }
            metricFamily = parser.parse();
        }
    }
}
