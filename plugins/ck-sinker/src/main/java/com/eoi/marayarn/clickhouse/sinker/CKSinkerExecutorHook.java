package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.ExecutorHook;
import com.eoi.marayarn.MaraApplicationMaster;

import java.util.Map;

public class CKSinkerExecutorHook implements ExecutorHook {
    public static final String CALL_BACK_ENDPOINT = "METRIC_PUSH_GATEWAY_ADDRS";

    private MaraApplicationMaster applicationMaster;

    public CKSinkerExecutorHook(MaraApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public void hookPrepareExecutorEnvironments(Map<String, String> envs, String containerId) {
        if (applicationMaster.metricsReporter != null) {
            envs.put(CALL_BACK_ENDPOINT,
                    this.applicationMaster.trackingUrl + "/cks/prom/write/" + containerId);
        }
    }
}
