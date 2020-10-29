package com.eoi.marayarn.logstash;

import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.HandlerErrorException;
import com.eoi.marayarn.http.ProcessResult;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;

/**
 * mock as an elasticsearch server
 * handle logstash/ and logstash/_xpack
 * act as elasticsearch 6
 */
public class ClusterHandler implements Handler {
    @Override
    public Map<String, String> match(String uri, HttpMethod method) {
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        Map<String, String> uriParams = new HashMap<>();
        if (uri.equals("/logstash/_xpack")) {
            uriParams.put("xpack", "true");
        } else if (uri.startsWith("/logstash")) {
            uriParams.put("cluster", "true");
        } else {
            return null;
        }
        return uriParams;
    }

    @Override
    public ProcessResult process(Map<String, String> urlParams, HttpMethod method, byte[] body)
            throws HandlerErrorException {
        if (urlParams.containsKey("cluster")) {
            return ProcessResult.jsonProcessResult(ClusterInfo.buildMockClusterInfo());
        } else if (urlParams.containsKey("xpack")) {
            return ProcessResult.jsonProcessResult(XPackInfo.buildMockXPackInfo());
        }
        throw new HandlerErrorException(HttpResponseStatus.NOT_FOUND, new Exception("no match handler function"));
    }
}
