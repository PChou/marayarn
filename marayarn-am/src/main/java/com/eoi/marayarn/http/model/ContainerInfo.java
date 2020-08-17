package com.eoi.marayarn.http.model;

import com.eoi.marayarn.YarnAllocator;

public class ContainerInfo {
    public String Id;
    public String nodeId;
    public String nodeHttpAddress;
    public String logUrl;
    public int vcore;
    public int memory;
    public int state;

    public static ContainerInfo fromContainer(YarnAllocator.ContainerAndState cas, String webSchema, String user) {
        if (cas == null)
            return null;
        ContainerInfo containerInfo = new ContainerInfo();
        containerInfo.Id = cas.container.getId().toString();
        containerInfo.nodeId = cas.container.getNodeId().toString();
        containerInfo.nodeHttpAddress = cas.container.getNodeHttpAddress();
        containerInfo.logUrl = String.format("%s://%s/node/containerlogs/%s/%s", webSchema, containerInfo.nodeHttpAddress, containerInfo.Id, user);
        containerInfo.vcore = cas.container.getResource().getVirtualCores();
        containerInfo.memory = cas.container.getResource().getMemory();
        containerInfo.state = cas.state;
        return containerInfo;
    }
}
