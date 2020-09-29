package com.eoi.marayarn.web.exception;


import com.eoi.marayarn.web.enums.MsgCode;

public class BizException extends Exception {
    private String code;
    private String message;

    public BizException(MsgCode msgCode) {
       this(msgCode.code,msgCode.massage);
    }

    public BizException(String code, String message) {
        super(message);
        this.code = code;
        this.message = message;
    }

    public BizException(MsgCode msgCode,Throwable cause) {
        this(msgCode.code,msgCode.massage,cause);
    }

    public BizException(String code, String message,Throwable cause) {
        super(message,cause);
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
