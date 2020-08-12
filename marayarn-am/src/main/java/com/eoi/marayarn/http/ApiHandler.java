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

    abstract Map<String, String> apiMatch(String uri, HttpMethod method);
}
