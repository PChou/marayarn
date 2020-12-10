package com.eoi.marayarn;

public class ContainerInfo {
    private String id;
    private String nodeId;
    private String nodeHttpAddress;
    private String logUrl;
    private int vcore;
    private int memory;
    private int state;

    public String getId() {
        return id;
    }

    public ContainerInfo setId(String id) {
        this.id = id;
        return this;
    }

    public String getNodeId() {
        return nodeId;
    }

    public ContainerInfo setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public String getNodeHttpAddress() {
        return nodeHttpAddress;
    }

    public ContainerInfo setNodeHttpAddress(String nodeHttpAddress) {
        this.nodeHttpAddress = nodeHttpAddress;
        return this;
    }

    public String getLogUrl() {
        return logUrl;
    }

    public ContainerInfo setLogUrl(String logUrl) {
        this.logUrl = logUrl;
        return this;
    }

    public int getVcore() {
        return vcore;
    }

    public ContainerInfo setVcore(int vcore) {
        this.vcore = vcore;
        return this;
    }

    public int getMemory() {
        return memory;
    }

    public ContainerInfo setMemory(int memory) {
        this.memory = memory;
        return this;
    }

    public int getState() {
        return state;
    }

    public ContainerInfo setState(int state) {
        this.state = state;
        return this;
    }
}
