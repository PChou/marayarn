package com.eoi.marayarn.http;

import com.eoi.marayarn.ApplicationMaster;
import com.eoi.marayarn.YarnAllocator;
import com.eoi.marayarn.http.model.ApplicationInfo;
import com.eoi.marayarn.http.model.ContainerInfo;
import com.eoi.marayarn.http.model.ErrorResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.data.Json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApplicationHandler extends ApiHandler {
    final ApplicationMaster applicationMaster;

    static final String INSTANCES_KEY = "instances";

    public ApplicationHandler(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    Map<String, String> apiMatch(String uri, HttpMethod method) {
        Map<String, String> uriParams = new HashMap<>();
        if (method == HttpMethod.GET && uri.equals("/app")) {
            uriParams.put("key", "app");
            return uriParams;
        } else if (method == HttpMethod.POST && uri.equals("/app/scale")) {
            uriParams.put("key", "scale");
            return uriParams;
        }
        return null;
    }

    @Override
    public byte[] process(Map<String, String> urlParams, HttpMethod method, byte[] body)
            throws HandlerErrorException {
        try {
            if (urlParams.get("key").equals("app") && method == HttpMethod.GET) {
                ApplicationInfo appInfo = new ApplicationInfo();
                appInfo.applicationId = applicationMaster.applicationAttemptId.getApplicationId().toString();
                appInfo.startTime = applicationMaster.amClient.getStartTime();
                appInfo.trackingUrl = applicationMaster.trackingUrl;
                appInfo.arguments = applicationMaster.arguments;
                appInfo.numTotalExecutors = applicationMaster.allocator.targetNumExecutors;
                appInfo.numRunningExecutors = applicationMaster.allocator.getRunningExecutors();
                appInfo.numAllocatedExecutors = applicationMaster.allocator.getAllocatedExecutors();
                appInfo.numPendingExecutors = applicationMaster.allocator.getNumPendingAllocate();
                List<ContainerInfo> containers = new ArrayList<>();
                for (YarnAllocator.ContainerAndState cas: applicationMaster.allocator.getContainers()) {
                    containers.add(ContainerInfo.fromContainer(cas));
                }
                appInfo.containers = containers;
                return JsonUtil._mapper.writeValueAsBytes(appInfo);
            } else if (urlParams.get("key").equals("scale") && method == HttpMethod.POST) {
                int scale = 0;
                try {
                    Map<String, Object> request = JsonUtil._mapper.readValue(body, new TypeReference<Map<String, Object>>() {});
                    Object instances = request.get(INSTANCES_KEY);
                    scale = Integer.parseInt(instances.toString());
                } catch (Exception ex) {
                    throw new HandlerErrorException(HttpResponseStatus.BAD_REQUEST, "invalid parameter `instances`");
                }
                applicationMaster.allocator.scale(scale);
                ErrorResponse response = new ErrorResponse();
                response.errMessage = "ok";
                return JsonUtil._mapper.writeValueAsBytes(response);
            }
        } catch (Exception ex) {
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex.toString());
        }
        throw new HandlerErrorException(HttpResponseStatus.NOT_FOUND, "no match handler function");
    }
}
