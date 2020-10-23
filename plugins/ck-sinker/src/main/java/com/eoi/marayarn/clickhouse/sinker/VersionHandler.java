package com.eoi.marayarn.clickhouse.sinker;

import com.eoi.marayarn.http.Handler;
import com.eoi.marayarn.http.HandlerErrorException;
import com.eoi.marayarn.http.ProcessResult;
import io.netty.handler.codec.http.HttpMethod;

import java.util.HashMap;
import java.util.Map;

public class VersionHandler implements Handler {
    @Override
    public Map<String, String> match(String uri, HttpMethod method) {
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        if (!uri.startsWith("/cks")) {
            return null;
        }
        return new HashMap<>();
    }

    @Override
    public ProcessResult process(Map<String, String> urlParams, HttpMethod method, byte[] body) throws HandlerErrorException {
        return ProcessResult.jsonProcessResult(new Version("1.1-SNAPSHOT"));
    }
}
