package com.eoi.marayarn.http;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PageHandler implements Handler {
    public static final String WEB_ROOT = "/web";

    private static final Map<String, String> mime = new HashMap<>();
    static {
        mime.put(".html", "text/html; charset=UTF-8");
        mime.put(".css", "text/css");
        mime.put(".js", "text/javascript");
        mime.put(".woff", "font/woff");
        mime.put(".ttf", "font/ttf");
    }

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
            throw new HandlerErrorException(HttpResponseStatus.NOT_FOUND, new Exception("resource not found"));
        try {
            byte[] content = IOUtils.toByteArray(indexHtmlStream);
            int i = urlParams.get("path").lastIndexOf('.');
            if (i < 0) {
                return new ProcessResult(content, "text/html; charset=UTF-8");
            } else {
                return new ProcessResult(content,
                        mime.getOrDefault(urlParams.get("path").substring(i), "text/html; charset=UTF-8"));
            }
        } catch (IOException ex) {
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
        }
    }
}
