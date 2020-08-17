package com.eoi.marayarn.http;

import io.netty.handler.codec.http.HttpResponseStatus;

public class HandlerErrorException extends Exception {
    public HttpResponseStatus status;
    public String message;
    public HandlerErrorException(HttpResponseStatus status, String message) {
        this.status = status;
        this.message = message;
    }
}