package com.eoi.marayarn.http;

import com.eoi.marayarn.ApplicationMasterArguments;
import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.YarnAllocator;
import com.eoi.marayarn.http.model.AckResponse;
import com.eoi.marayarn.http.model.ApplicationInfo;
import com.eoi.marayarn.http.model.ApplicationRequirement;
import com.eoi.marayarn.http.model.ContainerInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApplicationHandler extends ApiHandler {
    final MaraApplicationMaster applicationMaster;

    static final String INSTANCES_KEY = "instances";

    public ApplicationHandler(MaraApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    Map<String, String> apiMatch(String uri, HttpMethod method) {
        Map<String, String> uriParams = new HashMap<>();
        if (method == HttpMethod.GET && uri.equals("/app")) {
            uriParams.put("action", "get");
        } else if (method == HttpMethod.POST && uri.equals("/app")) {
            uriParams.put("action", "update");
        } else if (method == HttpMethod.POST && uri.equals("/app/scale")) {
            uriParams.put("action", "scale");
        } else if (method == HttpMethod.POST && uri.equals("/app/stop")) {
            uriParams.put("action", "stop");
        } else {
            uriParams = null;
        }
        return uriParams;
    }

    @Override
    public byte[] apiProcess(Map<String, String> urlParams, HttpMethod method, byte[] body)
            throws HandlerErrorException {
        try {
            String action = urlParams.get("action");
            if (action.equals("get")) {
                ApplicationInfo appInfo = getApplicationIfo();
                return JsonUtil._mapper.writeValueAsBytes(appInfo);
            } else if (action.equals("scale")) {
                int scale = 0;
                try {
                    Map<String, Object> request = JsonUtil._mapper.readValue(body, new TypeReference<Map<String, Object>>() {});
                    Object instances = request.get(INSTANCES_KEY);
                    scale = Integer.parseInt(instances.toString());
                } catch (Exception ex) {
                    throw new HandlerErrorException(HttpResponseStatus.BAD_REQUEST, "invalid parameter `instances`");
                }
                applicationMaster.allocator.scale(scale);
                return JsonUtil._mapper.writeValueAsBytes(AckResponse.OK);
            } else if (action.equals("stop")) {
                applicationMaster.terminate();
                return JsonUtil._mapper.writeValueAsBytes(AckResponse.OK);
            } else if (action.equals("update")) {
                ApplicationRequirement requirement = JsonUtil._mapper.readValue(body, ApplicationRequirement.class);
                ApplicationMasterArguments arguments =
                        requirement.mergeApplicationMasterArguments(applicationMaster.allocator.arguments);
                applicationMaster.allocator.update(arguments);
                return JsonUtil._mapper.writeValueAsBytes(AckResponse.OK);
            }
        } catch (Exception ex) {
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex.toString());
        }
        throw new HandlerErrorException(HttpResponseStatus.NOT_FOUND, "no match handler function");
    }

    private ApplicationInfo getApplicationIfo() {
        ApplicationInfo appInfo = new ApplicationInfo();
        appInfo.applicationId = applicationMaster.applicationAttemptId.getApplicationId().toString();
        appInfo.startTime = applicationMaster.amClient.getStartTime();
        appInfo.trackingUrl = applicationMaster.trackingUrl;
        appInfo.arguments = applicationMaster.arguments;
        appInfo.numTotalExecutors = applicationMaster.allocator.targetNumExecutors;
        appInfo.numRunningExecutors = applicationMaster.allocator.getRunningExecutors();
        appInfo.numAllocatedExecutors = applicationMaster.allocator.getAllocatedExecutors();
        appInfo.numPendingExecutors = applicationMaster.allocator.getNumPendingAllocate();
        appInfo.logUrl = applicationMaster.getAMContainerLogs();
        List<ContainerInfo> containers = new ArrayList<>();
        for (YarnAllocator.ContainerAndState cas: applicationMaster.allocator.getContainers()) {
            containers.add(ContainerInfo.fromContainer(cas, applicationMaster.webSchema, applicationMaster.user));
        }
        appInfo.containers = containers;
        return appInfo;
    }
}
