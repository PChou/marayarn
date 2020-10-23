package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.HandlerFactory;

import java.util.Arrays;

public class CKSinkerAMHandlerFactory implements HandlerFactory {
    private MaraApplicationMaster applicationMaster;

    public CKSinkerAMHandlerFactory(MaraApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public Iterable<Handler> getHandlers() {
        return Arrays.asList(new PromWriteHandler(this.applicationMaster), new VersionHandler());
    }
}