package com.eoi.marayarn.http;

public class ProcessResult {
    public byte[] body;
    public String contentType;

    public ProcessResult(byte[] body, String contentType) {
        this.body = body;
        this.contentType = contentType;
    }
}
