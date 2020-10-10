package com.eoi.marayarn;

import java.util.List;

public class ScaleRequest {
    private Integer instances;
    private List<String> containerIds;

    public Integer getInstances() {
        return instances;
    }

    public ScaleRequest setInstances(Integer instances) {
        this.instances = instances;
        return this;
    }

    public List<String> getContainerIds() {
        return containerIds;
    }

    public ScaleRequest setContainerIds(List<String> containerIds) {
        this.containerIds = containerIds;
        return this;
    }
}
