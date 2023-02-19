package com.eoi.marayarn.prometheus;

import com.eoi.marayarn.MetricsReporter;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

public abstract class PrometheusMetricReporter implements MetricsReporter {

    protected static final Logger log = LoggerFactory.getLogger(PrometheusMetricReporter.class);

    private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");

    private static final Function<String, String> CHARACTER_FILTER =
            input -> replaceInvalidChars(input);

    static String replaceInvalidChars(final String input) {
        // https://prometheus.io/docs/instrumenting/writing_exporters/
        // Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to
        // an underscore.
        return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
    }

    private String endpoint;
    private String jobName;

    private final Map<String, Map<List<String>, Collector>> labeledCollectorMap = new HashMap<>();

    @Override
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    public abstract void start();

    @Override
    public abstract void stop();

    @Override
    public abstract void flush();

    @Override
    public void putGauge(String metricName, Map<String, String> labels, double value) {
        List<String> labelKeys = new ArrayList<>(labels.keySet());
        Collections.sort(labelKeys);
        List<String> labelValues = new ArrayList<>();
        for (String labelKey: labelKeys) {
            labelValues.add(labels.get(labelKey));
        }
        Gauge collector = (Gauge) genCollector(metricName, labelKeys, (name, lbl) -> Gauge.build(name, name)
                .labelNames(lbl).create());
        collector.labels(labelValues.toArray(new String[0])).set(value);
    }

    @Override
    public void putCounter(String metricName, Map<String, String> labels, double inc) {
        List<String> labelKeys = new ArrayList<>(labels.keySet());
        Collections.sort(labelKeys);
        List<String> labelValues = new ArrayList<>();
        for (String labelKey: labelKeys) {
            labelValues.add(labels.get(labelKey));
        }
        Counter collector = (Counter) genCollector(metricName, labelKeys,
                (name, lbl) -> Counter.build(name, name).labelNames(lbl).create());
        collector.labels(labelValues.toArray(new String[0])).inc(inc);
    }

    @Override
    public void  putFullCounter(String metricName, Map<String, String> labels, double value) {
        List<String> labelKeys = new ArrayList<>(labels.keySet());
        Collections.sort(labelKeys);
        List<String> labelValues = new ArrayList<>();
        for (String labelKey: labelKeys) {
            labelValues.add(labels.get(labelKey));
        }
        Counter collector = (Counter) genCollector(metricName, labelKeys,
                (name, lbl) -> Counter.build(name, name).labelNames(lbl).create());
        String[] lvs = labelValues.toArray(new String[0]);
        collector.labels(lvs).inc(value - collector.labels(lvs).get());
    }

    @Override
    public void putHistogram(String metricName,
            Map<String, String> labels, List<Bucket> bucketList, double sampleSum, long sampleCount) {
        List<String> labelKeys = new ArrayList<>(labels.keySet());
        Collections.sort(labelKeys);
        List<String> labelValues = new ArrayList<>();
        for (String labelKey: labelKeys) {
            labelValues.add(labels.get(labelKey));
        }
        HistogramProxyCollector collector = (HistogramProxyCollector) genCollector(metricName, labelKeys,
                (name, lbl) -> HistogramProxyCollector.build(name, name).labelNames(lbl).create());
        String[] lvs = labelValues.toArray(new String[0]);
        collector.labels(lvs).update(bucketList, sampleSum, sampleCount);
    }

    @Override
    public void putSummary(String metricName,
            Map<String, String> labels, List<Quantile> bucketList, double sampleSum, long sampleCount) {
        List<String> labelKeys = new ArrayList<>(labels.keySet());
        Collections.sort(labelKeys);
        List<String> labelValues = new ArrayList<>();
        for (String labelKey: labelKeys) {
            labelValues.add(labels.get(labelKey));
        }
        SummaryProxyCollector collector = (SummaryProxyCollector) genCollector(metricName, labelKeys,
                (name, lbl) -> SummaryProxyCollector.build(name, name).labelNames(lbl).create());
        String[] lvs = labelValues.toArray(new String[0]);
        collector.labels(lvs).update(bucketList, sampleSum, sampleCount);
    }

    private Collector genCollector(String metricName, List<String> labelKeys,
                             BiFunction<String, String[], Collector> generator) {
        String validMetricName = CHARACTER_FILTER.apply(metricName);
        Map<List<String>, Collector> collectorMap = labeledCollectorMap.computeIfAbsent(validMetricName, k -> new HashMap<>());
        Collector collector = collectorMap.get(labelKeys);
        if (collector == null) {
            collector = generator.apply(validMetricName, labelKeys.toArray(new String[0]));
            collectorMap.put(labelKeys, collector);
            log.info("Register collector for {}({}) with label keys: {}" ,
                    validMetricName, collector.getClass().getCanonicalName() ,String.join(",", labelKeys));
            CollectorRegistry.defaultRegistry.register(collector);
        }
        return collector;
    }
}
