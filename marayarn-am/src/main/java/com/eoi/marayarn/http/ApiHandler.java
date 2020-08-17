package com.eoi.marayarn.http;

import io.netty.handler.codec.http.HttpMethod;
import java.util.Map;

/**
 * pre match the uri start with `/api`, and strip that
 */
public abstract class ApiHandler implements Handler {
    private final static String API_PREFIX = "/api";

    @Override
    public Map<String, String> match(String uri, HttpMethod method) {
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        if (!uri.startsWith(API_PREFIX)) {
            return null;
        }
        return apiMatch(uri.substring(API_PREFIX.length()), method);
    }

    @Override
    public ProcessResult process(Map<String, String> urlParams, HttpMethod method, byte[] body)
            throws HandlerErrorException {
        byte[] content = apiProcess(urlParams, method, body);
        return new ProcessResult(content, "application/json; charset=UTF-8");
    }

    abstract Map<String, String> apiMatch(String uri, HttpMethod method);
    abstract byte[] apiProcess(Map<String, String> urlParams, HttpMethod method, byte[] body)
            throws HandlerErrorException;
}
