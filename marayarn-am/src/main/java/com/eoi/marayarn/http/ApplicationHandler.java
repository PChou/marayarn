package com.eoi.marayarn.http;

import com.eoi.marayarn.ApplicationMasterArguments;
import com.eoi.marayarn.ApplicationMasterPlugin;
import com.eoi.marayarn.MaraApplicationMaster;
import com.eoi.marayarn.YarnAllocator;
import com.eoi.marayarn.http.model.*;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApplicationHandler extends ApiHandler {
    final MaraApplicationMaster applicationMaster;

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
                ScaleRequest request = JsonUtil._mapper.readValue(body, ScaleRequest.class);
                applicationMaster.allocator.scaleAndKill(request.instances, request.containerIds);
                return JsonUtil._mapper.writeValueAsBytes(AckResponse.OK);
            } else if (action.equals("stop")) {
                applicationMaster.terminate();
                return JsonUtil._mapper.writeValueAsBytes(AckResponse.OK);
            } else if (action.equals("update")) {
                ApplicationRequirement requirement = JsonUtil._mapper.readValue(body, ApplicationRequirement.class);
                ApplicationMasterArguments arguments =
                        requirement.mergeApplicationMasterArguments(applicationMaster.allocator.getArguments());
                applicationMaster.allocator.update(arguments);
                return JsonUtil._mapper.writeValueAsBytes(AckResponse.OK);
            }
        } catch (Exception ex) {
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
        }
        throw new HandlerErrorException(HttpResponseStatus.NOT_FOUND, new Exception("no match handler function"));
    }

    private ApplicationInfo getApplicationIfo() {
        ApplicationInfo appInfo = new ApplicationInfo();
        appInfo.applicationId = applicationMaster.applicationAttemptId.getApplicationId().toString();
        appInfo.startTime = applicationMaster.amClient.getStartTime();
        appInfo.trackingUrl = applicationMaster.trackingUrl;
        // set dashboardUrl if needed
        String grafana = System.getenv(MaraApplicationMaster.GRAFANA_URL_ENV_KEY);
        if (grafana != null && !grafana.isEmpty()) {
            List<ApplicationMasterPlugin> pluginList = applicationMaster.getApplicationPlugins();
            for (ApplicationMasterPlugin plugin: pluginList) {
                String gdId = plugin.grafanaDashboardId();
                if (gdId != null && !gdId.isEmpty()) {
                    appInfo.dashboardUrl =
                            String.format("%s/d/%s?var-applicationId=%s&kiosk=tv", grafana, gdId, appInfo.applicationId);
                }
            }
        }
        appInfo.arguments = applicationMaster.arguments;
        appInfo.numTotalExecutors = applicationMaster.allocator.targetNumExecutors;
        appInfo.numRunningExecutors = applicationMaster.allocator.getRunningExecutors();
        appInfo.numAllocatedExecutors = applicationMaster.allocator.getAllocatedExecutors();
        appInfo.numPendingExecutors = applicationMaster.allocator.getPendingAllocations();
        appInfo.logUrl = applicationMaster.getAMContainerLogs();
        appInfo.containers = new ArrayList<>();
        for (YarnAllocator.ContainerAndState cas: applicationMaster.allocator.getContainers()) {
            appInfo.containers.add(ContainerInfo.fromContainer(cas, applicationMaster.webSchema, applicationMaster.user));
        }
        appInfo.completedContainers = new ArrayList<>();
        for (YarnAllocator.ContainerAndState cas: applicationMaster.allocator.getCompletedContainers()) {
            appInfo.completedContainers.add(ContainerInfo.fromContainer(cas, applicationMaster.webSchema, applicationMaster.user));
        }
        return appInfo;
    }
}
