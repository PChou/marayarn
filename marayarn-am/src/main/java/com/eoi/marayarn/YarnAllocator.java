package com.eoi.marayarn;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class YarnAllocator {
    private static Logger logger = LoggerFactory.getLogger(YarnAllocator.class);
    // All RM requests are issued with same priority : we do not (yet) have any distinction between
    // request types (like map/reduce in hadoop for example)
    private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(1);
    private Configuration conf;
    private AMRMClient<AMRMClient.ContainerRequest> amrmClient;
    private NMClient nmClient;
    private ApplicationAttemptId applicationAttemptId;
    private ApplicationMasterArguments arguments;
    private ThreadPoolExecutor launcherPool;
    private Resource containerResource;
    public Map<ContainerId, ContainerAndState> allocatedContainers;

    // 状态
    public int targetNumExecutors = 0;

    public YarnAllocator(
            Configuration configuration,
            AMRMClient<AMRMClient.ContainerRequest> amClient,
            ApplicationAttemptId applicationAttemptId,
            ApplicationMasterArguments arguments) {
        this.conf = configuration;
        this.amrmClient = amClient;
        this.applicationAttemptId = applicationAttemptId;
        this.arguments = arguments;
        this.launcherPool = new ThreadPoolExecutor(
                // max pool size of Integer.MAX_VALUE is ignored because we use an unbounded queue
                25, Integer.MAX_VALUE,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("ContainerLauncher #%d").setDaemon(true).build());
        this.targetNumExecutors = arguments.numExecutors;
        this.containerResource = Resource.newInstance(arguments.executorMemory, arguments.executorCores);
        this.allocatedContainers = new HashMap<>();
        this.nmClient = NMClient.createNMClient();
        this.nmClient.init(this.conf);
        this.nmClient.start();
    }

    public synchronized int getRunningExecutors() {
        int num = 0;
        for (Map.Entry<ContainerId, ContainerAndState> c: allocatedContainers.entrySet()) {
            if (c.getValue().state == 1) {
                num++;
            }
        }
        return num;
    }

    public synchronized int getAllocatedExecutors() {
        return allocatedContainers.size();
    }

    public synchronized int getNumPendingAllocate() {
        // getMatchingRequests返回的是相应priority下的所有capability <= 指定资源的申请，相同的capability在一个子集合中
        // 假设提交的容器资源都是一致的priority和capability，其实只有一个集合
        // cancelPendingRequests需要这个逻辑前提
        return amrmClient.getMatchingRequests(RM_REQUEST_PRIORITY, "*", containerResource)
                .stream().mapToInt(Collection::size).sum();
    }

    public synchronized List<ContainerAndState> getContainers() {
        List<ContainerAndState> containers = new ArrayList<>();
        for (Map.Entry<ContainerId, ContainerAndState> container: allocatedContainers.entrySet()) {
            containers.add(container.getValue());
        }
        return containers;
    }

    public synchronized void allocateResources() throws Exception {
        updateResourceRequests();
        final float progressIndicator = 0.1f;
        AllocateResponse allocateResponse =  amrmClient.allocate(progressIndicator);
        List<Container> acs = allocateResponse.getAllocatedContainers();
        handleAllocatedContainers(acs);
        List<ContainerStatus> completedContainers = allocateResponse.getCompletedContainersStatuses();
        handleCompletedContainers(completedContainers);
    }

    public void handleAllocatedContainers(List<Container> acs) {
        if (acs == null)
            return;
        for (Container container: acs) {
            if (!allocatedContainers.containsKey(container.getId())) {
                allocatedContainers.put(container.getId(), new ContainerAndState(container, 0));
                logger.info("Canceling 1 pending requests since got 1 allocated");
                cancelPendingRequests(1);
            }
        }

        if (allocatedContainers.size() == targetNumExecutors) {
            for (Map.Entry<ContainerId, ContainerAndState> c: allocatedContainers.entrySet()) {
                ContainerAndState containerState = c.getValue();
                Container container = containerState.container;
                if (containerState.state == 1) continue;
                // start the container and set state flag to 1
                ExecutorRunnable executorRunnable = new ExecutorRunnable(this.conf, container, this.arguments.commandLine, this.nmClient);
                launcherPool.execute(executorRunnable);
                logger.info("Launching container {} on host {}", c.getKey(), container.getNodeId().getHost());
                containerState.state = 1;
            }
        }
    }

    public void handleCompletedContainers(List<ContainerStatus> completedContainers) {
        if (completedContainers == null)
            return;
        for (ContainerStatus status: completedContainers) {
            ContainerId containerId = status.getContainerId();
            logger.warn("Completed container {} (state: {}, exit status: {}.",
                    containerId,
                    status.getState(),
                    status.getExitStatus());
            allocatedContainers.remove(status.getContainerId());
            if (status.getExitStatus() == -103 || status.getExitStatus() == -104) {
                logger.warn("Container killed by YARN for exceeding memory limits");
            } else if (status.getExitStatus() != 0) {
                logger.warn("Container marked as failed: {}. Exit status: {}. Diagnostics: {}",
                        containerId,
                        status.getExitStatus(),
                        status.getDiagnostics());
            }
        }
    }

    public void updateResourceRequests() {
        // get the pending container request
        int pendingSize = getNumPendingAllocate();
        int missing = targetNumExecutors - pendingSize - allocatedContainers.size();
        if (missing > 0) {
            // 表示还需要加入更多的container
            logger.info("Requesting {} executor containers, each with resource {}", missing, containerResource);
            for (int i = 0; i < missing; i++) {
                AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(containerResource, null, null, RM_REQUEST_PRIORITY);
                amrmClient.addContainerRequest(containerRequest);
            }
        } else if (missing < 0) {
            // 表示应该缩减pending或者kill现有的container
            int sub = -missing;
            if (sub <= pendingSize) {
                // 如果pending的足够扣减的话，直接扣减sub
                logger.info("Canceling {} pending requests since exceed {} allocated", sub, sub);
                cancelPendingRequests(sub);
                // pendingSize -= sub;
            } else {
                // 如果pending的不够扣减的话，先扣减所有的pending，然后release已经分配的未运行的，最后kill一些已经运行的
                logger.info("Canceling {} pending requests since exceed {} allocated", pendingSize, sub);
                cancelPendingRequests(pendingSize);
                sub -= pendingSize;
                // pendingSize = 0;
                List<ContainerId> removingContainerIds = new ArrayList<>();
                for (Map.Entry<ContainerId, ContainerAndState> c: allocatedContainers.entrySet()) {
                    if (c.getValue().state == 0) {
                        amrmClient.releaseAssignedContainer(c.getKey());
                        removingContainerIds.add(c.getKey());
                        sub--;
                    }
                    if (sub == 0) {
                        break;
                    }
                }
                if (sub > 0) {
                    for (Map.Entry<ContainerId, ContainerAndState> c: allocatedContainers.entrySet()) {
                        if (c.getValue().state == 1) {
                            try {
                                // TODO: stop的时候考虑尽量分散节点
                                logger.info("Stopping and release container {} on node {}", c.getKey(), c.getValue().container.getNodeId());
                                nmClient.stopContainer(c.getKey(), c.getValue().container.getNodeId());
                                amrmClient.releaseAssignedContainer(c.getKey());
                                removingContainerIds.add(c.getKey());
                                sub--;
                            } catch (Exception ex) {
                                logger.error("Failed to stop container {}", c.getKey(), ex);
                            }
                        }
                        if (sub == 0) {
                            break;
                        }
                    }
                }
                for (ContainerId id: removingContainerIds) {
                    allocatedContainers.remove(id);
                }
                if (sub > 0) {
                    logger.error("Still leave {} container need to cut after cancel pending, allocated and runnning", sub);
                }
            }
        }
    }

    private void cancelPendingRequests(int num) {
        List<? extends Collection<AMRMClient.ContainerRequest>> pendingRequests
                = amrmClient.getMatchingRequests(RM_REQUEST_PRIORITY, "*", containerResource);
        if (pendingRequests.isEmpty()) {
            return;
        }
        Iterator<AMRMClient.ContainerRequest> requests = pendingRequests.get(0).iterator();
        List<AMRMClient.ContainerRequest> removing = new ArrayList<>();
        int i = 0;
        while(requests.hasNext() && i < num) {
            removing.add(requests.next());
            i++;
        }
        for (AMRMClient.ContainerRequest rm: removing) {
            amrmClient.removeContainerRequest(rm);
        }
    }

    /**
     * scale the number of executors to target num
     */
    public synchronized void scale(int num) {
        if (num < 0) {
            return;
        }
        targetNumExecutors = num;
    }


    public static class ContainerAndState {
        public Container container;
        // 0: not launched, 1: launched
        public int state;

        public ContainerAndState(Container container, int state) {
            this.container = container;
            this.state = state;
        }
    }
}
