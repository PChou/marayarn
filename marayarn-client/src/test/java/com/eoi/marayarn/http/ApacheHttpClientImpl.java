package com.eoi.marayarn.http;

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.LinkedHashMap;
import java.util.Map;

public class ApacheHttpClientImpl implements IHttpClient {

    private final int connectTimeourMs;
    private final int responseTimeoutMs;
    private final CloseableHttpClient client;

    public ApacheHttpClientImpl() {
        this(30000, 300000);
    }

    public ApacheHttpClientImpl(int connectTimeourMs, int responseTimeoutMs) {
        this.connectTimeourMs = connectTimeourMs;
        this.responseTimeoutMs = responseTimeoutMs;
        final RequestConfig config = RequestConfig.custom()
                .setSocketTimeout(responseTimeoutMs).setConnectTimeout(connectTimeourMs).build();
        this.client = HttpClients.custom().setDefaultRequestConfig(config).build();
    }

    @Override
    public Response execute(Request request) throws Exception {
        HttpUriRequest uriRequest = null;
        URIBuilder uriBuilder = new URIBuilder(request.getEndpoint());
        if (request.getParameters() != null) {
            for (Map.Entry<String, String> param: request.getParameters().entrySet()) {
                uriBuilder.addParameter(param.getKey(), param.getValue());
            }
        }
        if (Request.GET.equals(request.getMethod())) {
            uriRequest = new HttpGet(uriBuilder.build());
        } else if (Request.POST.equals(request.getMethod())) {
            uriRequest = new HttpPost(uriBuilder.build());
            ((HttpPost)uriRequest).setEntity(new ByteArrayEntity(request.getBody()));
        }
        if (uriRequest == null) {
            return null;
        }
        for (Map.Entry<String, String> header: request.getHeaders().entrySet()) {
            uriRequest.addHeader(header.getKey(), header.getValue());
        }
        CloseableHttpResponse response = this.client.execute(uriRequest);
        int status = response.getStatusLine().getStatusCode();
        Map<String, String> headers = new LinkedHashMap<>();
        if(response.getAllHeaders() != null && response.getAllHeaders().length > 0) {
            for(int i = 0; i < response.getAllHeaders().length; i++) {
                Header header = response.getAllHeaders()[i];
                headers.put(header.getName(), header.getValue());
            }
        }
        byte[] body = EntityUtils.toByteArray(response.getEntity());
        return new Response(status, headers, body);
    }

    @Override
    public void close() {
        try {
            this.client.close();
        } catch (Exception ignored) { }
    }
}

