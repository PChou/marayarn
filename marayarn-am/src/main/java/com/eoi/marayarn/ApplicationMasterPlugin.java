package com.eoi.marayarn;

import com.eoi.marayarn.http.HandlerFactory;

public interface ApplicationMasterPlugin {
    /**
     * name of plugin
     */
    String name();
    /**
     * return HandlerFactory for http handler
     * @return
     */
    HandlerFactory handlerFactory();

    /**
     * return an executor hook if need to inject the execution process of container
     * @return
     */
    ExecutorHook getExecutorHook();

    /**
     * start used to initialize plugin when application master, and will be called before any other
     * @param applicationMaster
     */
    void start(MaraApplicationMaster applicationMaster);

    /**
     * stop plugin
     */
    void stop();
}
