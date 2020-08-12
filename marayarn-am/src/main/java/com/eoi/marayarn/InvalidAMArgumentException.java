package com.eoi.marayarn;

public class InvalidAMArgumentException extends Exception {
    public InvalidAMArgumentException(String message) {
        super(message);
    }

    public InvalidAMArgumentException(String message, Throwable ex) {
        super(message, ex);
    }
}
