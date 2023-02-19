package com.eoi.marayarn;

import java.util.Map;

public interface ExecutorHook {

    /**
     * change envs if needed before execute container
     * envs will be set as container environments before execute the container
     *
     * if no need to change, just return
     * @param envs
     */
    void hookPrepareExecutorEnvironments(Map<String, String> envs, String containerId);
}
