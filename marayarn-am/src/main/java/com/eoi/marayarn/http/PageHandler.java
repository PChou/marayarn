package com.eoi.marayarn.http;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class PageHandler implements Handler {
    public static final String WEB_ROOT = "/web";

    @Override
    public Map<String, String> match(String uri, HttpMethod method) {
        String localPath = uri;
        if (localPath == null || localPath.isEmpty() || localPath.equals("/")) {
            localPath = "/index.html";
        }
        return Collections.singletonMap("path", WEB_ROOT + localPath);
    }

    @Override
    public ProcessResult process(Map<String, String> urlParams, HttpMethod method, byte[] body)
            throws HandlerErrorException {
        InputStream indexHtmlStream = this.getClass().getResourceAsStream(urlParams.get("path"));
        if (indexHtmlStream == null)
            throw new HandlerErrorException(HttpResponseStatus.NOT_FOUND, "resource not found");
        try {
            int byteCount = indexHtmlStream.available();
            byte[] content = new byte[byteCount];
            indexHtmlStream.read(content);
            return new ProcessResult(content, "text/html; charset=UTF-8");
        } catch (IOException ex) {
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex.toString());
        }
    }
}
