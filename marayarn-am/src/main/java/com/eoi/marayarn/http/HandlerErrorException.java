package com.eoi.marayarn.http;

import io.netty.handler.codec.http.HttpResponseStatus;

public class HandlerErrorException extends Exception {
    public HttpResponseStatus status;
    public Throwable e;

    public HandlerErrorException(HttpResponseStatus status, Throwable e) {
        this.status = status;
        this.e = e;
    }
}