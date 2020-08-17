package com.eoi.marayarn.http.model;

public class AckResponse {
    public String code;
    public String message;

    public static AckResponse OK = AckResponse.build("0", "ok");

    public static AckResponse build(String code, String message) {
        AckResponse ackResponse = new AckResponse();
        ackResponse.code = code;
        ackResponse.message = message;
        return ackResponse;
    }
}
