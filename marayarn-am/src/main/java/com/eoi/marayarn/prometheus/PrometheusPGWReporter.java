package com.eoi.marayarn.prometheus;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PrometheusPGWReporter extends PrometheusMetricReporter {
    protected static final Logger log = LoggerFactory.getLogger(PrometheusPGWReporter.class);

    private PushGateway pushGateway;

    @Override
    public void start() {
        pushGateway = new PushGateway(getEndpoint());
        log.info("Starting PrometheusMetricReporter with {{}, jobName:{}}", getEndpoint(), getJobName());
    }

    @Override
    public void flush() {
        if (pushGateway == null) {
            return;
        }
        try {
            log.info("Push metrics to prometheus gateway");
            pushGateway.push(CollectorRegistry.defaultRegistry, getJobName());
        } catch (IOException e) {
            log.warn(
                    "Failed to push metrics to PushGateway with jobName {}",
                    getJobName(),
                    e);
        }
    }

    @Override
    public void stop() {
        if (pushGateway != null) {
            try {
                pushGateway.delete(getJobName());
            } catch (IOException e) {
                log.warn(
                        "Failed to delete metrics from PushGateway with jobName {}.",
                        getJobName(),
                        e);
            }
        }
        CollectorRegistry.defaultRegistry.clear();
    }
}
