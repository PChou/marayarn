package com.eoi.marayarn.logstash;

import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.HandlerFactory;

import java.util.Arrays;

public class LogstashHandlerFactory implements HandlerFactory {

    private MaraApplicationMaster applicationMaster;

    public LogstashHandlerFactory(MaraApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public Iterable<Handler> getHandlers() {
        return Arrays.asList(new MonitoringHandler(this.applicationMaster), new ClusterHandler());
    }
}
