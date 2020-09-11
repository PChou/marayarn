package com.eoi.marayarn.http;

public interface IHttpClient {
    Response execute(Request request) throws Exception;
    void close();
}
