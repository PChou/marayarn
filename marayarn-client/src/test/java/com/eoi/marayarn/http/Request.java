package com.eoi.marayarn.http;

import java.util.Map;

public class Request {
    private final String method;
    private final String endpoint;
    private final Map<String, String> parameters;
    private final Map<String, String> headers;
    private final byte[] body;

    public static final String GET = "GET";
    public static final String POST = "POST";

    public Request(String method, String endpoint, Map<String, String> parameters, Map<String, String> headers, byte[] body) {
        this.method = method;
        this.endpoint = endpoint;
        this.parameters = parameters;
        this.headers = headers;
        this.body = body;
    }

    public String getMethod() {
        return method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body;
    }
}

