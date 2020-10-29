package com.eoi.marayarn.logstash;

import com.eoi.marayarn.ExecutorHook;
import com.eoi.marayarn.MaraApplicationMaster;

import java.util.Map;

public class LogstashExecutorHook implements ExecutorHook {
    public static final String CALL_BACK_ENDPOINT = "METRICS_PUSH_GATEWAY_ADDR";
    private MaraApplicationMaster applicationMaster;

    public LogstashExecutorHook(MaraApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public void hookPrepareExecutorEnvironments(Map<String, String> envs) {
        envs.put(CALL_BACK_ENDPOINT, this.applicationMaster.trackingUrl + "/logstash");
    }
}
