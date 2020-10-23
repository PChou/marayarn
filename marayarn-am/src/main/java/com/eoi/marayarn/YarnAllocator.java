package com.eoi.marayarn;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class YarnAllocator {
    private static Logger logger = LoggerFactory.getLogger(YarnAllocator.class);
    // All RM requests are issued with same priority : we do not (yet) have any distinction between
    // request types (like map/reduce in hadoop for example)
    private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(1);
    private static final int RESERVE_COMPLETED_CONTAINER_SIZE = 50;
    private static final int PENDING_TIMEOUT_SEC = 60;
    private Configuration conf;
    // user used to do yarn client operation
    private UserGroupInformation ugi = null;
    private YarnClient yarnClient;
    private AMRMClient<AMRMClient.ContainerRequest> amrmClient;
    private NMClient nmClient;
    private ApplicationAttemptId applicationAttemptId;
    private ApplicationMasterArguments arguments;
    private List<ApplicationMasterPlugin> applicationPlugins;
    private ThreadPoolExecutor launcherPool;
    private Resource containerResource;
    private Map<ContainerId, ContainerAndState> allocatedContainers;
    private Queue<ContainerAndState> completedContainers;
    // record the container location constraints
    private List<ContainerLocation> locations;

    // 状态
    public int targetNumExecutors = 0;
    // restarting状态下，container正在被终止，targetNumExecutors固定在0
    // update和scale引起的numExecutors变化只会体现在pendingNumExecutors
    // 当restarting完成后，restarting重置为true，将pendingNumExecutors赋值给targetNumExecutors
    private boolean restarting = false;
    private int pendingNumExecutors = 0;

    // for test
    protected YarnAllocator(ApplicationMasterArguments arguments) {
        this.arguments = arguments;
    }

    public YarnAllocator(
            Configuration configuration,
            AMRMClient<AMRMClient.ContainerRequest> amClient,
            ApplicationAttemptId applicationAttemptId,
            ApplicationMasterArguments arguments,
            List<ApplicationMasterPlugin> applicationPlugins) {
        this.conf = configuration;
        this.amrmClient = amClient;
        this.applicationAttemptId = applicationAttemptId;
        this.arguments = arguments;
        this.applicationPlugins = applicationPlugins;
        this.launcherPool = new ThreadPoolExecutor(
                // max pool size of Integer.MAX_VALUE is ignored because we use an unbounded queue
                25, Integer.MAX_VALUE,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("ContainerLauncher #%d").setDaemon(true).build());
        this.targetNumExecutors = arguments.numExecutors;
        this.containerResource = Resource.newInstance(arguments.executorMemory, arguments.executorCores);
        this.allocatedContainers = new HashMap<>();
        this.completedContainers = new LinkedList<>();
        this.nmClient = NMClient.createNMClient();
        this.nmClient.init(this.conf);
        this.nmClient.start();
        try {
            if (!Utils.StringEmpty(arguments.principal) && !Utils.StringEmpty(arguments.keytab)) {
                logger.info("Login using {}", arguments.principal);
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(arguments.principal, arguments.keytab);
            } else {
                ugi = UserGroupInformation.getCurrentUser();
            }
            this.locations = ugi.doAs((PrivilegedExceptionAction<List<ContainerLocation>>) () -> {
                yarnClient = YarnClient.createYarnClient();
                yarnClient.init(conf);
                yarnClient.start();
                List<NodeReport> candidates = yarnClient.getNodeReports();
                return initLocation(candidates, arguments);
            });
        } catch (Exception e) {
            logger.warn("Failed to initialize ugi or get node report", e);
            this.locations = initLocation(null, arguments);
        }
    }

    public ApplicationMasterArguments getArguments() {
        return arguments;
    }

    /**
     * get all allocated containers
     * @return
     */
    public synchronized List<ContainerAndState> getContainers() {
        List<ContainerAndState> containers = new ArrayList<>();
        for (Map.Entry<ContainerId, ContainerAndState> container: allocatedContainers.entrySet()) {
            containers.add(container.getValue());
        }
        return containers;
    }

    /**
     * get all completed containers from completedContainers queue
     * @return
     */
    public synchronized List<ContainerAndState> getCompletedContainers() {
        return new ArrayList<>(this.completedContainers);
    }

    /**
     * get running executor count
     * @return
     */
    public synchronized int getRunningExecutors() {
        int num = 0;
        for (Map.Entry<ContainerId, ContainerAndState> c: allocatedContainers.entrySet()) {
            if (c.getValue().state == 1) {
                num++;
            }
        }
        return num;
    }

    /**
     * get allocated container count
     * @return
     */
    public synchronized int getAllocatedExecutors() {
        return allocatedContainers.size();
    }

    /**
     * get pending allocation container count
     * @return
     */
    public synchronized int getPendingAllocations() {
        return locations.stream().mapToInt(ContainerLocation::getPendingCount).sum();
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

    public synchronized void scaleAndKill(int num, List<String> killingContainerIds) {
        if (killingContainerIds != null && killingContainerIds.size() > 0) {
            List<ContainerId> ids = killingContainerIds.stream()
                    .map(ConverterUtils::toContainerId).collect(Collectors.toList());
            for (ContainerId id: ids) {
                ContainerAndState cs = allocatedContainers.get(id);
                if (cs != null) {
                    try {
                        nmClient.stopContainer(cs.container.getId(), cs.container.getNodeId());
                    } catch (Exception ex) {
                        logger.error(String.format("Failed to stop container %s", id), ex);
                    }
                } else {
                    logger.warn("No match container found for {}", id);
                }
            }
        }
        scale(num);
    }

    public synchronized void update(ApplicationMasterArguments newArguments) {
        if (newArguments == null)
            return;
        if (targetNumExecutors == newArguments.numExecutors
                && Utils.StringEquals(this.arguments.commandLine, newArguments.commandLine)
                && this.arguments.executorMemory == newArguments.executorMemory
                && this.arguments.executorCores == newArguments.executorCores
                && Utils.StringEquals(this.arguments.constraints, newArguments.constraints)) {
            // no change
            return;
        }
        this.arguments.commandLine = newArguments.commandLine;
        this.arguments.executorMemory = newArguments.executorMemory;
        this.arguments.executorCores = newArguments.executorCores;
        this.arguments.numExecutors = newArguments.numExecutors;
        this.arguments.constraints = newArguments.constraints;
        this.targetNumExecutors = 0; // clear all container
        this.restarting = true;
        this.pendingNumExecutors = newArguments.numExecutors;
    }


    /**
     * allocateResources will do:
     * 1. update resource request
     * 2. collect allocated containers
     * 3. collect and handle completed containers
     * @throws Exception
     */
    protected synchronized void allocateResources() throws Exception {
        updateResourceRequests();
        final float progressIndicator = 0.1f;
        AllocateResponse allocateResponse = amrmClient.allocate(progressIndicator);
        List<Container> acs = allocateResponse.getAllocatedContainers();
        handleAllocatedContainers(acs);
        List<ContainerStatus> completedContainers = allocateResponse.getCompletedContainersStatuses();
        handleCompletedContainers(completedContainers);
    }

        /**
         * 初始化locations, 初始化YarnAllocator以及update时可能触发
         * @param candidates nodereport
         * @param arguments 参数中的constraints需要考虑
         * @return
         */
    private List<ContainerLocation> initLocation(List<NodeReport> candidates, ApplicationMasterArguments arguments) {
        if (Utils.StringEmpty(arguments.constraints) || Utils.ListEmpty(candidates)) {
            return Collections.singletonList(new ContainerLocation(null, null));
        }
        try {
            return Locality.judgeLocationBy(
                    candidates, arguments.numExecutors, arguments.executorCores, arguments.executorMemory, arguments.constraints);
        } catch (InvalidConstraintsSettingException e) {
            logger.warn("Failed to initialize locations by {}", arguments.constraints, e);
            return Collections.singletonList(new ContainerLocation(null, null));
        }
    }

    /**
     * get num of available for all locations
     * calculate topMostCount - allocatedCount - pendingCount for each location
     * @return
     */
    private synchronized long getNumAvailable() {
        long available = 0;
        for (ContainerLocation loc: locations) {
            available += (loc.getTopMostCount() - loc.getAllocatedCount() - loc.getPendingCount());
        }
        return available;
    }


    /**
     * match the coresponding ContainerLocation by the container's node or rack
     * @param container
     * @return return null if not found
     */
    private ContainerLocation getMatchedLocation(Container container) {
        if (locations.size() < 1)
            return null;
        final String node = container.getNodeId().getHost();
        for (ContainerLocation loc: locations) {
            if (loc.getNode() != null) {
                for (int i = 0; i < loc.getNode().length; i++) {
                    if (node.equals(loc.getNode()[i])) {
                        return loc;
                    }
                }
            } else if (loc.getRack() != null) {
                final String rack = RackResolver.resolve(node).getNetworkLocation();
                for (int i = 0; i < loc.getRack().length; i++) {
                    if (rack.equals(loc.getRack()[i])) {
                        return loc;
                    }
                }
            } else {
                // both node and rack are null, means ANY
                return loc;
            }
        }
        return null;
    }

    /**
     * match the coresponding ContainerLocation by containerId
     * @param containerId
     * @return return null if not found
     */
    private ContainerLocation getMatchedLocation(ContainerId containerId) {
        if (locations.size() < 1)
            return null;
        for (ContainerLocation loc: locations) {
            Container e = loc.getDetail().get(containerId);
            if (e != null) {
                return loc;
            }
        }
        return null;
    }

    private void pushToCompletedContainers(ContainerAndState cas) {
        if (completedContainers.size() >= RESERVE_COMPLETED_CONTAINER_SIZE) {
            completedContainers.poll();
        }
        completedContainers.add(cas);
    }


    private void handleAllocatedContainers(List<Container> acs) {
        if (acs == null)
            return;
        for (Container container: acs) {
            if (!allocatedContainers.containsKey(container.getId())) {
                ContainerLocation containerLocation = getMatchedLocation(container);
                if (containerLocation == null) {
                    logger.error("Failed to match location request by container");
                } else {
                    // 保存container引用，增加allocated和pending的数量，重置pendingDelay
                    containerLocation.putDetail(container.getId(), container);
                    containerLocation.incrAllocatedCount();
                    containerLocation.decrPendingCount();
                    containerLocation.setPendingDelay(0);
                    logger.info("Canceling 1 pending requests since got 1 allocated");
                    cancelPendingRequests(containerLocation, 1);
                }
                allocatedContainers.put(container.getId(), new ContainerAndState(container, 0));
            }
        }

        if (allocatedContainers.size() == targetNumExecutors) {
            if (restarting && targetNumExecutors == 0) {
                // reset targetNumExecutors using pendingNumExecutors
                targetNumExecutors = pendingNumExecutors;
                pendingNumExecutors = 0;
                containerResource = Resource.newInstance(arguments.executorMemory, arguments.executorCores);
                try {
                    List<NodeReport> nodeReports = ugi.doAs((PrivilegedExceptionAction<List<NodeReport>>) () -> yarnClient.getNodeReports());
                    logger.info("Calculating new locality for nodes: {}",
                            nodeReports.stream().map(NodeReport::getNodeId).map(NodeId::getHost)
                                    .reduce("", (a, b) -> a + "," + b));
                    locations = initLocation(nodeReports, arguments);
                    for (ContainerLocation loc: locations) {
                        logger.info("{}", loc);
                    }
                } catch (Exception e) {
                    logger.warn("Failed to calculate the locality", e);
                    locations = initLocation(null, arguments);
                }
                restarting = false;
            } else {
                // really execute container when targetNumExecutors is met
                for (Map.Entry<ContainerId, ContainerAndState> c: allocatedContainers.entrySet()) {
                    ContainerAndState containerState = c.getValue();
                    Container container = containerState.container;
                    if (containerState.state == 1) continue;
                    // start the container and set state flag to 1
                    ExecutorRunnable executorRunnable =
                            new ExecutorRunnable(this.conf, container, this.arguments.commandLine, this.nmClient,
                                    applicationPlugins.stream().map(ApplicationMasterPlugin::getExecutorHook).collect(Collectors.toList()));
                    launcherPool.execute(executorRunnable);
                    logger.info("Launching container {} on host {}", c.getKey(), container.getNodeId().getHost());
                    containerState.state = 1;
                }
            }
        }
    }

    private void handleCompletedContainers(List<ContainerStatus> completedContainers) {
        if (Utils.ListEmpty(completedContainers))
            return;
        for (ContainerStatus status: completedContainers) {
            ContainerId containerId = status.getContainerId();
            logger.warn("Completed container {} (state: {}, exit status: {}).",
                    containerId,
                    status.getState(),
                    status.getExitStatus());
            ContainerAndState removed = allocatedContainers.get(status.getContainerId());
            allocatedContainers.remove(status.getContainerId());
            if (removed != null) {
                pushToCompletedContainers(removed);
            }
            ContainerLocation location = getMatchedLocation(status.getContainerId());
            if (location != null) {
                location.decrAllocatedCount();
                location.removeDetail(status.getContainerId());
            }
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

    private void updateResourceRequests() {
        // get the pending container request
        int pendingSize = getPendingAllocations();
        int missing = targetNumExecutors - pendingSize - allocatedContainers.size();
        if (missing > 0) {
            // 表示还需要加入更多的container
            // 先根据locations进行均匀分配
            while (missing > 0 && getNumAvailable() > 0) {
                for (int j = 0; j < locations.size() && missing > 0; j++) {
                    ContainerLocation loc = locations.get(j);
                    if (locations.size() > 1 && loc.getPendingDelay() > PENDING_TIMEOUT_SEC) {
                        // 如果超过一个loc，并且当前这个loc的pendingDelay > PENDING_TIMEOUT_SEC
                        // 则不要在这个loc上分配，并重置loc的pendingDelay
                        loc.setPendingDelay(0);
                        continue;
                    }
                    // go through the preferred location
                    int available = loc.getTopMostCount() - loc.getAllocatedCount() - loc.getPendingCount();
                    // skip if no more can be allocated
                    if (available <= 0)
                        continue;
                    logger.info("Requesting {} executor containers at {}, each with resource {}", 1, loc, containerResource);
                    AMRMClient.ContainerRequest containerRequest =
                            new AMRMClient.ContainerRequest(containerResource,
                                    loc.getNode(),
                                    loc.getRack(),
                                    RM_REQUEST_PRIORITY,
                                    //relaxLocality 如果不设置为false的话 yarn不一定会根据需求分配container
                                    //当node和rack都是null的时候，必须设置relax为true
                                    loc.getNode() == null && loc.getRack() == null,
                                    null);
                    amrmClient.addContainerRequest(containerRequest);
                    loc.incrPendingCount();
                    missing -= 1;
                }
            }
            if (missing > 0) {
                logger.error("Not all required container can be requested, probably because the constraints");
                for (ContainerLocation loc: locations) {
                    logger.info("{}", loc);
                }
            }
        } else if (missing < 0) {
            // 表示应该缩减pending或者kill现有的container
            int sub = -missing;
            if (sub <= pendingSize) {
                // 如果pending的足够扣减的话，直接扣减sub
                logger.info("Canceling {} pending requests since exceed {} allocated", sub, sub);
                int reserve = sub;
                // 这里尽量针对每个location平均去cancel
                while (reserve > 0) {
                    for (ContainerLocation loc : locations) {
                        if (loc.getPendingCount() > 0 && reserve > 0) {
                            cancelPendingRequests(loc, 1);
                            loc.decrPendingCount();
                            reserve--;
                        }
                    }
                }
            } else {
                // 如果pending的不够扣减的话，先扣减所有的pending，然后release已经分配的未运行的，最后kill一些已经运行的
                logger.info("Canceling {} pending requests since exceed {} allocated", pendingSize, sub);
                for (ContainerLocation loc : locations) {
                    if (loc.getPendingCount() > 0) {
                        cancelPendingRequests(loc, loc.getPendingCount());
                        loc.setPendingCount(0);
                    }
                }
                sub -= pendingSize;
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
                                // stop should trigger handleCompleted
                                // removingContainerIds.add(c.getKey());
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
                    ContainerLocation loc = getMatchedLocation(id);
                    if (loc != null) {
                        loc.removeDetail(id);
                        loc.decrAllocatedCount();
                    }
                }
                if (sub > 0) {
                    logger.error("Still leave {} container need to cut after cancel pending, allocated and runnning", sub);
                }
            }
        } else { // missing == 0
            // check目前是否有pending的
            if (pendingSize == 0) return;
            // 如果locations只有一个那么不存在切换的可能
            if (locations.size() < 2) return;
            for (ContainerLocation loc: locations) {
                if (loc.getPendingCount() > 0) {
                    // 增加所有pending的loc的delay值
                    loc.incrPendingDelay(5);
                    // 如果delay大于超时时间, 则取消这个loc上的pending任务
                    // 等待下个周期的分配
                    if (loc.getPendingDelay() > PENDING_TIMEOUT_SEC) {
                        logger.warn("Pending delay is over {}s, cancel {} requests at {}",
                                PENDING_TIMEOUT_SEC, loc.getPendingCount(), loc);
                        loc.decrPendingCount();
                        cancelPendingRequests(loc, loc.getPendingCount());
                    }
                }
            }
        }
    }

    private void cancelPendingRequests(ContainerLocation location, int num) {
        if (location == null)
            return;
        AMRMClient.ContainerRequest removingRequest =
                new AMRMClient.ContainerRequest(containerResource, location.getNode(), location.getRack(), RM_REQUEST_PRIORITY);
        for (int i = 0; i < num; i++) {
            amrmClient.removeContainerRequest(removingRequest);
        }
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
