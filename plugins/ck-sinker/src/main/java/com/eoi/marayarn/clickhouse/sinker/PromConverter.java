package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.clickhouse.sinker.parse.types.*;
import com.eoi.marayarn.prometheus.protobuf.Types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PromConverter {
    public static final String METRIC_NAME_KEY = "__name__";

    // TODO: filter NaN
    public static void  convertAndFill(List<Types.TimeSeries> timeSeriesList, Metric metric, long timestamp, String yarnId) {
        if (metric == null || timeSeriesList == null)
            return;
        List<Types.Label> commonLabel = new ArrayList<>(8);
        for (Map.Entry<String, String> label: metric.getLabels().entrySet()) {
            Types.Label tag = Types.Label.newBuilder().setName(label.getKey()).setValue(label.getValue()).build();
            commonLabel.add(tag);
        }
        commonLabel.add(Types.Label.newBuilder().setName("job").setValue(yarnId).build());
        if (metric instanceof Counter) {
            Types.TimeSeries ts = new Types.TimeSeries();
            ts.setSamples(Collections.singletonList(
                    Types.Sample.newBuilder()
                            .setTimestamp(timestamp)
                            .setValue(((Counter) metric).getValue()).build()
            ));
            commonLabel.add(Types.Label.newBuilder().setName(METRIC_NAME_KEY).setValue(metric.getName()).build());
            ts.setLabels(commonLabel);
            timeSeriesList.add(ts);
        } else if (metric instanceof Gauge) {
            Types.TimeSeries ts = new Types.TimeSeries();
            ts.setSamples(Collections.singletonList(
                    Types.Sample.newBuilder()
                            .setTimestamp(timestamp)
                            .setValue(((Gauge) metric).getValue()).build()
            ));
            commonLabel.add(Types.Label.newBuilder().setName(METRIC_NAME_KEY).setValue(metric.getName()).build());
            ts.setLabels(commonLabel);
            timeSeriesList.add(ts);
        } else if (metric instanceof Histogram) {
            Histogram histogram = (Histogram) metric;
            // each bucket should be a TimeSeries
            for (Histogram.Bucket bucket: histogram.getBuckets()) {
                List<Types.Label> defaultLabel = new ArrayList<>(8);
                defaultLabel.addAll(commonLabel);
                defaultLabel.add(Types.Label.newBuilder().setName(METRIC_NAME_KEY).setValue(metric.getName() + "_bucket").build());
                // TODO check Inf
                defaultLabel.add(Types.Label.newBuilder().setName("le").setValue(String.format("%f", bucket.getUpperBound())).build());
                Types.TimeSeries ts = new Types.TimeSeries();
                ts.setSamples(Collections.singletonList(
                        Types.Sample.newBuilder()
                                .setTimestamp(timestamp)
                                .setValue(bucket.getCumulativeCount()).build()
                ));
                ts.setLabels(defaultLabel);
                timeSeriesList.add(ts);
            }
            // sum and count should be two seperate TimeSeries
            List<Types.Label> defaultLabelSum = new ArrayList<>(8);
            defaultLabelSum.addAll(commonLabel);
            defaultLabelSum.add(Types.Label.newBuilder().setName(METRIC_NAME_KEY).setValue(metric.getName() + "_sum").build());
            Types.TimeSeries tsSum = new Types.TimeSeries();
            tsSum.setSamples(Collections.singletonList(
                    Types.Sample.newBuilder()
                            .setTimestamp(timestamp)
                            .setValue(histogram.getSampleSum()).build()
            ));
            tsSum.setLabels(defaultLabelSum);
            timeSeriesList.add(tsSum);

            List<Types.Label> defaultLabelCount = new ArrayList<>(8);
            defaultLabelCount.addAll(commonLabel);
            defaultLabelCount.add(Types.Label.newBuilder().setName(METRIC_NAME_KEY).setValue(metric.getName() + "_count").build());
            Types.TimeSeries tsCount = new Types.TimeSeries();
            tsCount.setSamples(Collections.singletonList(
                    Types.Sample.newBuilder()
                            .setTimestamp(timestamp)
                            .setValue(histogram.getSampleCount()).build()
            ));
            tsCount.setLabels(defaultLabelCount);
            timeSeriesList.add(tsCount);
        } else if (metric instanceof Summary) {
            Summary summary = (Summary) metric;
            for (Summary.Quantile quantile: summary.getQuantiles()) {
                List<Types.Label> defaultLabel = new ArrayList<>(8);
                defaultLabel.addAll(commonLabel);
                defaultLabel.add(Types.Label.newBuilder().setName(METRIC_NAME_KEY).setValue(metric.getName()).build());
                // TODO check Inf
                defaultLabel.add(Types.Label.newBuilder().setName("quantile").setValue(String.format("%f", quantile.getQuantile())).build());
                Types.TimeSeries ts = new Types.TimeSeries();
                ts.setSamples(Collections.singletonList(
                        Types.Sample.newBuilder()
                                .setTimestamp(timestamp)
                                .setValue(quantile.getValue()).build()
                ));
                ts.setLabels(defaultLabel);
                timeSeriesList.add(ts);
            }
            // sum and count should be two seperate TimeSeries
            List<Types.Label> defaultLabelSum = new ArrayList<>(8);
            defaultLabelSum.addAll(commonLabel);
            defaultLabelSum.add(Types.Label.newBuilder().setName(METRIC_NAME_KEY).setValue(metric.getName() + "_sum").build());
            Types.TimeSeries tsSum = new Types.TimeSeries();
            tsSum.setSamples(Collections.singletonList(
                    Types.Sample.newBuilder()
                            .setTimestamp(timestamp)
                            .setValue(summary.getSampleSum()).build()
            ));
            tsSum.setLabels(defaultLabelSum);
            timeSeriesList.add(tsSum);

            List<Types.Label> defaultLabelCount = new ArrayList<>(8);
            defaultLabelCount.addAll(commonLabel);
            defaultLabelCount.add(Types.Label.newBuilder().setName(METRIC_NAME_KEY).setValue(metric.getName() + "_count").build());
            Types.TimeSeries tsCount = new Types.TimeSeries();
            tsCount.setSamples(Collections.singletonList(
                    Types.Sample.newBuilder()
                            .setTimestamp(timestamp)
                            .setValue(summary.getSampleCount()).build()
            ));
            tsCount.setLabels(defaultLabelCount);
            timeSeriesList.add(tsCount);
        }
    }
}
