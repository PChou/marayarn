package com.eoi.marayarn;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;

public class AMClient implements Closeable {
    private static final int CONNECT_TIMEOUT = 2000;
    private static final int SOCKET_TIMEOUT = 10000;

    private final CloseableHttpClient httpClient;
    private final RequestConfig requestConfig;

    public AMClient() {
        this(HttpClients.createDefault(),
                RequestConfig.custom()
                        .setConnectTimeout(CONNECT_TIMEOUT)
                        .setSocketTimeout(SOCKET_TIMEOUT)
                        .build());
    }

    public AMClient(CloseableHttpClient httpClient, RequestConfig requestConfig) {
        this.httpClient = httpClient;
        this.requestConfig = requestConfig;
    }

    public ApplicationInfo getApplication(String trackingUrl) throws IOException {
        String url = Utils.urlJoin(trackingUrl, "/api/app");
        String responseBody = get(url);
        return JsonUtil.decode(responseBody, ApplicationInfo.class);
    }

    public AMResponse stopApplication(String trackingUrl) throws IOException {
        String url = Utils.urlJoin(trackingUrl, "/api/stop");
        String responseBody = post(url, null);
        return JsonUtil.decode(responseBody, AMResponse.class);
    }

    public AMResponse scaleApplication(String trackingUrl, ScaleRequest request) throws IOException {
        String url = Utils.urlJoin(trackingUrl, "/api/app/scale");
        String requestBody = JsonUtil.encode(request);
        String responseBody = post(url, requestBody);
        return JsonUtil.decode(responseBody, AMResponse.class);
    }

    public AMResponse updateApplication(String trackingUrl, UpdateRequest request) throws IOException {
        String url = Utils.urlJoin(trackingUrl, "/api/app/app");
        String requestBody = JsonUtil.encode(request);
        String responseBody = post(url, requestBody);
        return JsonUtil.decode(responseBody, AMResponse.class);
    }

    private String get(String url) throws IOException {
        HttpGet httpReq = new HttpGet(url);
        httpReq.setConfig(requestConfig);
        try(CloseableHttpResponse httpResponse = httpClient.execute(httpReq)) {
            HttpEntity responseEntity = httpResponse.getEntity();
            return readResponse(responseEntity);
        }
    }

    private String post(String url, String body) throws IOException {
        HttpPost httpReq = new HttpPost(url);
        httpReq.setConfig(requestConfig);
        if(body != null) {
            StringEntity requestEntity = new StringEntity(body);
            requestEntity.setContentType("application/json");
            httpReq.setEntity(requestEntity);
        }
        try(CloseableHttpResponse httpResponse = httpClient.execute(httpReq)) {
            HttpEntity responseEntity = httpResponse.getEntity();
            return readResponse(responseEntity);
        }
    }

    private String readResponse(HttpEntity entity) throws IOException {
        return EntityUtils.toString(entity, "UTF-8");
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
