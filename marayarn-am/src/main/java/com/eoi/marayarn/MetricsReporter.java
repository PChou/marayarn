package com.eoi.marayarn;

import com.eoi.marayarn.prometheus.util.Util;

import java.util.List;
import java.util.Map;

public interface MetricsReporter {

    class Bucket {
        private final Double upperBound;
        private final long cumulativeCount;

        public Bucket(Double upperBound, long cumulativeCount) {
            this.upperBound = upperBound;
            this.cumulativeCount = cumulativeCount;
        }

        public Double getUpperBound() {
            return upperBound;
        }

        public long getCumulativeCount() {
            return cumulativeCount;
        }

        @Override
        public String toString() {
            return String.format("%s:%d", Util.convertDoubleToString(upperBound), cumulativeCount);
        }
    }

    class Quantile {
        private final double quantile;
        private final double value;

        public Quantile(double quantile, double value) {
            this.quantile = quantile;
            this.value = value;

        }

        public double getQuantile() {
            return quantile;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.format("%s:%s", Util.convertDoubleToString(quantile), Util.convertDoubleToString(value));
        }
    }


    /**
     * remote endpoint that store metrics
     * @param endpoint
     */
    void setEndpoint(String endpoint);

    void setJobName(String jobName);

    /**
     * start the reporter
     */
    void start();

    /**
     * stop the reporter when application exit
     */
    void stop();

    /**
     * flush metrics to remote
     */
    void flush();

    /**
     * add/update metric of type Gauge
     * @param metricName
     * @param labels
     * @param value
     */
    void putGauge(String metricName, Map<String, String> labels, double value);

    /**
     * add/update metric of type Count
     * @param metricName
     * @param labels
     * @param inc
     */
    void putCounter(String metricName, Map<String, String> labels, double inc);

    /**
     * add/update metric of type Gauge, but calculate as counter
     * @param metricName
     * @param labels
     * @param value
     */
    void putFullCounter(String metricName, Map<String, String> labels, double value);

    void putHistogram(String metricName, Map<String, String> labels, List<Bucket> bucketList, double sampleSum, long sampleCount);

    void putSummary(String metricName, Map<String, String> labels, List<Quantile> bucketList, double sampleSum, long sampleCount);
}
