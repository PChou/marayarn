package com.eoi.marayarn;

public class AMResponse {
   private String code;
   private String message;

    public String getCode() {
        return code;
    }

    public AMResponse setCode(String code) {
        this.code = code;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public AMResponse setMessage(String message) {
        this.message = message;
        return this;
    }
}
