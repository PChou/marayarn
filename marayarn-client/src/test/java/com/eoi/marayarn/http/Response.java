package com.eoi.marayarn.http;

import java.util.Map;

public class Response {
    private final int status;
    private final Map<String, String> headers;
    private final byte[] body;

    public Response(int status, Map<String, String> headers, byte[] body) {
        this.status = status;
        this.headers = headers;
        this.body = body;
    }

    public int getStatus() {
        return status;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body;
    }
}
