package com.eoi.marayarn.prometheus;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

public class PrometheusMetricReporter {

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

    private final String endpoint;
    private final String jobName;

    private PushGateway pushGateway;

    private final Map<String, Map<List<String>, Collector>> labeledCollectorMap = new HashMap<>();

    public PrometheusMetricReporter(String endpoint, String jobName) {
        this.endpoint = endpoint;
        this.jobName = jobName;
    }

    public void start() {
        pushGateway = new PushGateway(endpoint);
        log.info("Starting PrometheusMetricReporter with {{}, jobName:{}}", endpoint, jobName);
    }

    public void stop() {
        if (pushGateway != null) {
            try {
                pushGateway.delete(jobName);
            } catch (IOException e) {
                log.warn(
                        "Failed to delete metrics from PushGateway with jobName {}.",
                        jobName,
                        e);
            }
        }
        CollectorRegistry.defaultRegistry.clear();
    }

    public void flush() {
        if (pushGateway == null) {
            return;
        }
        try {
            log.info("Push metrics to prometheus gateway");
            pushGateway.push(CollectorRegistry.defaultRegistry, jobName);
        } catch (IOException e) {
            log.warn(
                    "Failed to push metrics to PushGateway with jobName {}",
                    jobName,
                    e);
        }
    }

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

    private Collector genCollector(String metricName, List<String> labelKeys,
                             BiFunction<String, String[], Collector> generator) {
        String validMetricName = CHARACTER_FILTER.apply(metricName);
        Map<List<String>, Collector> collectorMap = labeledCollectorMap.computeIfAbsent(validMetricName, k -> new HashMap<>());
        Collector collector = collectorMap.get(labelKeys);
        if (collector == null) {
            collector = generator.apply(validMetricName, labelKeys.toArray(new String[0]));
            collectorMap.put(labelKeys, collector);
            log.info("Register collector for {} with label keys: {}" ,validMetricName, String.join(",", labelKeys));
            CollectorRegistry.defaultRegistry.register(collector);
        }
        return collector;
    }
}
