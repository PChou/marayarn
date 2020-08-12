package com.eoi.marayarn;

public class InvalidClientArgumentException extends Exception {
    public InvalidClientArgumentException(String message) {
        super(message);
    }

    public InvalidClientArgumentException(String message, Throwable ex) {
        super(message, ex);
    }
}
