package com.eoi.marayarn;

import java.util.List;

public class ApplicationInfo {
    private String applicationId;
    private long startTime;
    private String trackingUrl;
    private String logUrl;
    private AMArguments arguments;
    private int numRunningExecutors;
    private int numTotalExecutors;
    private int numAllocatedExecutors;
    private int numPendingExecutors;
    private List<ContainerInfo> containers;
    private List<ContainerInfo> completedContainers;

    public String getApplicationId() {
        return applicationId;
    }

    public ApplicationInfo setApplicationId(String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public long getStartTime() {
        return startTime;
    }

    public ApplicationInfo setStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public ApplicationInfo setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
        return this;
    }

    public String getLogUrl() {
        return logUrl;
    }

    public ApplicationInfo setLogUrl(String logUrl) {
        this.logUrl = logUrl;
        return this;
    }

    public AMArguments getArguments() {
        return arguments;
    }

    public ApplicationInfo setArguments(AMArguments arguments) {
        this.arguments = arguments;
        return this;
    }

    public int getNumRunningExecutors() {
        return numRunningExecutors;
    }

    public ApplicationInfo setNumRunningExecutors(int numRunningExecutors) {
        this.numRunningExecutors = numRunningExecutors;
        return this;
    }

    public int getNumTotalExecutors() {
        return numTotalExecutors;
    }

    public ApplicationInfo setNumTotalExecutors(int numTotalExecutors) {
        this.numTotalExecutors = numTotalExecutors;
        return this;
    }

    public int getNumAllocatedExecutors() {
        return numAllocatedExecutors;
    }

    public ApplicationInfo setNumAllocatedExecutors(int numAllocatedExecutors) {
        this.numAllocatedExecutors = numAllocatedExecutors;
        return this;
    }

    public int getNumPendingExecutors() {
        return numPendingExecutors;
    }

    public ApplicationInfo setNumPendingExecutors(int numPendingExecutors) {
        this.numPendingExecutors = numPendingExecutors;
        return this;
    }

    public List<ContainerInfo> getContainers() {
        return containers;
    }

    public ApplicationInfo setContainers(List<ContainerInfo> containers) {
        this.containers = containers;
        return this;
    }

    public List<ContainerInfo> getCompletedContainers() {
        return completedContainers;
    }

    public ApplicationInfo setCompletedContainers(List<ContainerInfo> completedContainers) {
        this.completedContainers = completedContainers;
        return this;
    }
}
