package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.ApplicationMasterPlugin;
import com.eoi.marayarn.ExecutorHook;
import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.http.HandlerFactory;

public class CKSinkerAMPlugin implements ApplicationMasterPlugin {
    public static final String INFLUXDB_URL_ENV_KEY = "INFLUXDB_URL";

    private MaraApplicationMaster applicationMaster;

    @Override
    public String name() {
        return "ck-sinker";
    }

    @Override
    public HandlerFactory handlerFactory() {
        return new CKSinkerAMHandlerFactory(applicationMaster);
    }

    @Override
    public ExecutorHook getExecutorHook() {
        return new CKSinkerExecutorHook(this.applicationMaster);
    }

    @Override
    public void start(MaraApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public void stop() {
        return;
    }
}
