package com.eoi.marayarn.prometheus;

import com.eoi.marayarn.MetricsReporter.Bucket;
import com.eoi.marayarn.prometheus.HistogramProxyCollector.HistogramChild;
import com.eoi.marayarn.prometheus.util.Util;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.SimpleCollector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

// 指标名+标签key确定一个Collector实例
// 内部不同的bucket upperbound区分统计
public class HistogramProxyCollector extends SimpleCollector<HistogramChild> {

    HistogramProxyCollector(HistogramProxyCollector.Builder builder) {
        super(builder);
    }

    public static HistogramProxyCollector.Builder build(String name, String help) {
        return new HistogramProxyCollector.Builder().name(name).help(help);
    }

    @Override
    protected HistogramChild newChild() {
        return new HistogramChild();
    }


    static class Values {
        List<Bucket> bucketList;
        double sampleSum;
        double sampleCount;
    }

    static class HistogramChild {
        private Map<List<Double>, Values> bucketMap = new HashMap<>();

        public void update(List<Bucket> bucketList, double sampleSum, long sampleCount) {
            if (bucketList == null) {
                return;
            }
            List<Double> bucketUpbounds = bucketList.stream().map(Bucket::getUpperBound).collect(Collectors.toList());
            Values values = bucketMap.computeIfAbsent(bucketUpbounds, k -> new Values());
            values.bucketList = bucketList;
            values.sampleCount = sampleCount;
            values.sampleSum = sampleSum;
        }
    }

    static class Builder extends SimpleCollector.Builder<HistogramProxyCollector.Builder, HistogramProxyCollector> {

        public Builder() {}

        @Override
        public HistogramProxyCollector create() {
            return new HistogramProxyCollector(this);
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> resultFamilySamples = new ArrayList<>();
        for (Entry<List<String>, HistogramChild> histogramChildEntry : this.children.entrySet()) {
            // 这一层是labelValues的区分
            List<String> labelValues = histogramChildEntry.getKey();
            HistogramChild child = histogramChildEntry.getValue();

            for (Entry<List<Double>, Values> listValuesEntry : child.bucketMap.entrySet()) {
                List<Sample> samples = new ArrayList<>();
                // bucket
                for (Bucket bucket : listValuesEntry.getValue().bucketList) {
                    List<String> finallabelKeys = new ArrayList<>(this.labelNames);
                    List<String> finalLabelValues = new ArrayList<>(labelValues);
                    finallabelKeys.add("le");
                    finalLabelValues.add(Util.convertDoubleToString(bucket.getUpperBound()));
                    samples.add(new Sample(this.fullname + "_bucket", finallabelKeys, finalLabelValues, bucket.getCumulativeCount()));
                }

                List<String> finallabelKeys = new ArrayList<>(this.labelNames);
                List<String> finalLabelValues = new ArrayList<>(labelValues);
                // sum
                samples.add(new Sample(this.fullname + "_sum", finallabelKeys, finalLabelValues, listValuesEntry.getValue().sampleSum));
                // count
                samples.add(new Sample(this.fullname + "_count", finallabelKeys, finalLabelValues, listValuesEntry.getValue().sampleCount));
                MetricFamilySamples familySamples = new MetricFamilySamples(this.fullname, Type.HISTOGRAM, this.help, samples);
                resultFamilySamples.add(familySamples);
            }
        }
        return resultFamilySamples;
    }
}
