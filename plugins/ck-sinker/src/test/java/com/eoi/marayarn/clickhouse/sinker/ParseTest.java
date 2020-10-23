package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.clickhouse.sinker.parse.text.TextPrometheusMetricDataParser;
import com.eoi.marayarn.clickhouse.sinker.parse.types.Histogram;
import com.eoi.marayarn.clickhouse.sinker.parse.types.Metric;
import com.eoi.marayarn.clickhouse.sinker.parse.types.MetricFamily;
import com.eoi.marayarn.clickhouse.sinker.protobuf.Types;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    private static boolean isMetricName(Types.TimeSeries ts, String name) {
        List<Types.Label> labels = ts.getLabelsList();
        return labels.stream().filter(l -> l.getName().equals("__name__")).findAny().get().getValue().equals(name);
    }

    @Test
    public void convertTest() throws Exception {
        InputStream inputStream = getClass().getResourceAsStream("/prometheusMetrics");
        TextPrometheusMetricDataParser parser = new TextPrometheusMetricDataParser(inputStream);
        MetricFamily metricFamily = parser.parse();
        List<Types.TimeSeries> tsList = new ArrayList<>(64);
        while (metricFamily != null) {
            for (Metric metric: metricFamily.getMetrics()) {
                PromConverter.convertAndFill(tsList, metric, 0, "fakeId");
            }
            metricFamily = parser.parse();
        }
        Assert.assertEquals(388, tsList.size());
        List<Types.TimeSeries> go_gc_duration_seconds = tsList.stream()
                .filter(ts -> ParseTest.isMetricName(ts, "go_gc_duration_seconds")).collect(Collectors.toList());
        Assert.assertEquals(5, go_gc_duration_seconds.size());
        List<Types.TimeSeries> go_gc_duration_seconds_sum = tsList.stream()
                .filter(ts -> ParseTest.isMetricName(ts, "go_gc_duration_seconds_sum")).collect(Collectors.toList());
        Assert.assertEquals(0.001578358, go_gc_duration_seconds_sum.get(0).getSamples(0).getValue(), 0.001);
        List<Types.TimeSeries> go_gc_duration_seconds_count = tsList.stream()
                .filter(ts -> ParseTest.isMetricName(ts, "go_gc_duration_seconds_count")).collect(Collectors.toList());
        Assert.assertEquals(10, go_gc_duration_seconds_count.get(0).getSamples(0).getValue(), 0.001);

        List<Types.TimeSeries> go_goroutines = tsList.stream()
                .filter(ts -> ParseTest.isMetricName(ts, "go_goroutines")).collect(Collectors.toList());
        Assert.assertEquals(435, go_goroutines.get(0).getSamples(0).getValue(), 0.001);

        List<Types.TimeSeries> go_memstats_frees_total = tsList.stream()
                .filter(ts -> ParseTest.isMetricName(ts, "go_memstats_frees_total")).collect(Collectors.toList());
        Assert.assertEquals(593453, go_memstats_frees_total.get(0).getSamples(0).getValue(), 0.001);

        List<Types.TimeSeries> prometheus_http_response_size_bytes_bucket = tsList.stream()
                .filter(ts -> ParseTest.isMetricName(ts, "prometheus_http_response_size_bytes_bucket")).collect(Collectors.toList());
        Assert.assertEquals(9, prometheus_http_response_size_bytes_bucket.size());

        List<Types.TimeSeries> prometheus_http_response_size_bytes_sum = tsList.stream()
                .filter(ts -> ParseTest.isMetricName(ts, "prometheus_http_response_size_bytes_sum")).collect(Collectors.toList());
        Assert.assertEquals(124885, prometheus_http_response_size_bytes_sum.get(0).getSamples(0).getValue(), 0.001);

        List<Types.TimeSeries> prometheus_http_response_size_bytes_count = tsList.stream()
                .filter(ts -> ParseTest.isMetricName(ts, "prometheus_http_response_size_bytes_count")).collect(Collectors.toList());
        Assert.assertEquals(17, prometheus_http_response_size_bytes_count.get(0).getSamples(0).getValue(), 0.001);

        List<Types.TimeSeries> prometheus_engine_query_duration_seconds = tsList.stream()
                .filter(ts -> ParseTest.isMetricName(ts, "prometheus_engine_query_duration_seconds")).collect(Collectors.toList());
        Assert.assertEquals(12, prometheus_engine_query_duration_seconds.size());
    }
}
