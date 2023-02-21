package com.eoi.marayarn.prometheus;

import com.eoi.marayarn.MetricsReporter.Quantile;
import com.eoi.marayarn.prometheus.SummaryProxyCollector.SummaryChild;
import com.eoi.marayarn.prometheus.util.Util;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.SimpleCollector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class SummaryProxyCollector extends SimpleCollector<SummaryChild> {
    SummaryProxyCollector(SummaryProxyCollector.Builder builder) {
        super(builder);
    }

    public static SummaryProxyCollector.Builder build(String name, String help) {
        return new SummaryProxyCollector.Builder().name(name).help(help);
    }

    @Override
    protected SummaryProxyCollector.SummaryChild newChild() {
        return new SummaryProxyCollector.SummaryChild();
    }


    static class Values {
        List<Quantile> bucketList;
        double sampleSum;
        double sampleCount;
    }

    static class SummaryChild {
        private Map<List<Double>, SummaryProxyCollector.Values> bucketMap = new HashMap<>();

        public void update(List<Quantile> bucketList, double sampleSum, long sampleCount) {
            if (bucketList == null) {
                return;
            }
            List<Double> bucketUpbounds = bucketList.stream().map(Quantile::getQuantile).collect(Collectors.toList());
            SummaryProxyCollector.Values values = bucketMap.computeIfAbsent(bucketUpbounds, k -> new SummaryProxyCollector.Values());
            values.bucketList = bucketList;
            values.sampleCount = sampleCount;
            values.sampleSum = sampleSum;
        }
    }

    static class Builder extends SimpleCollector.Builder<SummaryProxyCollector.Builder, SummaryProxyCollector> {

        public Builder() {}

        @Override
        public SummaryProxyCollector create() {
            return new SummaryProxyCollector(this);
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> resultFamilySamples = new ArrayList<>();
        for (Entry<List<String>, SummaryProxyCollector.SummaryChild> summaryChildEntry : this.children.entrySet()) {
            // 这一层是labelValues的区分
            List<String> labelValues = summaryChildEntry.getKey();
            SummaryProxyCollector.SummaryChild child = summaryChildEntry.getValue();

            for (Entry<List<Double>, SummaryProxyCollector.Values> listValuesEntry : child.bucketMap.entrySet()) {
                List<Sample> samples = new ArrayList<>();
                // bucket
                for (Quantile bucket : listValuesEntry.getValue().bucketList) {
                    List<String> finallabelKeys = new ArrayList<>(this.labelNames);
                    List<String> finalLabelValues = new ArrayList<>(labelValues);
                    finallabelKeys.add("quantile");
                    finalLabelValues.add(Util.convertDoubleToString(bucket.getQuantile()));
                    samples.add(new Sample(this.fullname, finallabelKeys, finalLabelValues, bucket.getValue()));
                }

                List<String> finallabelKeys = new ArrayList<>(this.labelNames);
                List<String> finalLabelValues = new ArrayList<>(labelValues);
                // sum
                samples.add(new Sample(this.fullname + "_sum", finallabelKeys, finalLabelValues, listValuesEntry.getValue().sampleSum));
                // count
                samples.add(new Sample(this.fullname + "_count", finallabelKeys, finalLabelValues, listValuesEntry.getValue().sampleCount));
                MetricFamilySamples familySamples = new MetricFamilySamples(this.fullname, Type.SUMMARY, this.help, samples);
                resultFamilySamples.add(familySamples);
            }
        }
        return resultFamilySamples;
    }
}
