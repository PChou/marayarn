package com.eoi.marayarn;

public class InvalidConstraintsSettingException extends Exception {
    public InvalidConstraintsSettingException(String message) {
        super(message);
    }

    public InvalidConstraintsSettingException(String message, Throwable ex) {
        super(message, ex);
    }
}
