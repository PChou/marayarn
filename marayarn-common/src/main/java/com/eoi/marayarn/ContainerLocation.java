package com.eoi.marayarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.util.*;


public class ContainerLocation {
    private String[] node;
    private String[] rack;
    // top most instance count that can be allocate in this node and rack combination
    private int topMostCount;
    // the number of instance that pending allocated
    private int pendingCount;
    // the number of instance that already allocated
    private int allocatedCount;
    // the accumulated seconds that pending request delayed
    private long pendingDelay;
    // allocated container
    private Map<ContainerId, Container> detail = new HashMap<>();

    public ContainerLocation() { }

    public ContainerLocation(String[] node, String[] rack) {
        this(node, rack, Integer.MAX_VALUE);
    }

    public ContainerLocation(String[] node, String[] rack, int count) {
        this.node = node;
        this.rack = rack;
        this.topMostCount = count;
    }

    public String[] getNode() {
        return node;
    }

    public void setNode(String[] node) {
        this.node = node;
    }

    public String[] getRack() {
        return rack;
    }

    public void setRack(String[] rack) {
        this.rack = rack;
    }

    public int getTopMostCount() {
        return topMostCount;
    }

    public void setTopMostCount(int topMostCount) {
        this.topMostCount = topMostCount;
    }

    public int getPendingCount() {
        return pendingCount;
    }

    public void setPendingCount(int pendingCount) {
        this.pendingCount = pendingCount;
    }

    public int getAllocatedCount() {
        return allocatedCount;
    }

    public void setAllocatedCount(int allocatedCount) {
        this.allocatedCount = allocatedCount;
    }

    public Map<ContainerId, Container> getDetail() {
        return detail;
    }

    public void putDetail(ContainerId containerId, Container container) {
        detail.put(containerId, container);
    }

    public void removeDetail(ContainerId containerId) {
        detail.remove(containerId);
    }

    public void incrAllocatedCount() {
        this.allocatedCount += 1;
    }

    public void decrAllocatedCount() {
        this.allocatedCount -= 1;
    }

    public void incrPendingCount() {
        this.pendingCount += 1;
    }

    public void decrPendingCount() {
        this.pendingCount -= 1;
    }

    public void decrPendingCount2(int c) {
        this.pendingCount -= c;
    }

    public long getPendingDelay() {
        return pendingDelay;
    }

    public void setPendingDelay(long pendingDelay) {
        this.pendingDelay = pendingDelay;
    }

    public void incrPendingDelay(long sec) {
        this.pendingDelay += sec;
    }

    @Override
    public String toString() {
        return "ContainerLocation{" +
                "node=" + Arrays.toString(node) +
                ", rack=" + Arrays.toString(rack) +
                ", (" + topMostCount +
                "/" + pendingCount + "/" + allocatedCount + ")" +
                '}';
    }
}
