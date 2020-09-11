package com.eoi.marayarn.http;

public class ApplicationMasterClient {

    private IHttpClient httpClient;
    private String trackingUrl;

    public ApplicationMasterClient(IHttpClient httpClient, String trackingUrl) {
        httpClient = httpClient;
        trackingUrl = trackingUrl;
    }
}
