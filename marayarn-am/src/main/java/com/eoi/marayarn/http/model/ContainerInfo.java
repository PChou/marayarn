package com.eoi.marayarn.http.model;

import com.eoi.marayarn.YarnAllocator;

public class ContainerInfo {
    public String Id;
    public String nodeId;
    public String nodeHttpAddress;
    public int vcore;
    public int memory;
    public int state;

    public static ContainerInfo fromContainer(YarnAllocator.ContainerAndState cas) {
        if (cas == null)
            return null;
        ContainerInfo containerInfo = new ContainerInfo();
        containerInfo.Id = cas.container.getId().toString();
        containerInfo.nodeId = cas.container.getNodeId().toString();
        containerInfo.nodeHttpAddress = cas.container.getNodeHttpAddress();
        containerInfo.vcore = cas.container.getResource().getVirtualCores();
        containerInfo.memory = cas.container.getResource().getMemory();
        containerInfo.state = cas.state;
        return containerInfo;
    }
}
