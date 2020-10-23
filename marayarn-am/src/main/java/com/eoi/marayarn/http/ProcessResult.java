package com.eoi.marayarn.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ProcessResult {
    public byte[] body;
    public String contentType;

    public ProcessResult(byte[] body, String contentType) {
        this.body = body;
        this.contentType = contentType;
    }

    public static ProcessResult jsonProcessResult(Object result) throws HandlerErrorException {
        try {
            return new ProcessResult(
                    JsonUtil._mapper.writeValueAsBytes(result), "application/json; charset=UTF-8");
        } catch (JsonProcessingException jpe) {
            throw new HandlerErrorException(HttpResponseStatus.INTERNAL_SERVER_ERROR, jpe);
        }
    }
}
