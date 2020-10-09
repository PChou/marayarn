package com.eoi.marayarn;

public class InvalidCliArgumentException extends Exception {

    public InvalidCliArgumentException(String message) {
        super(message);
    }

    public InvalidCliArgumentException(String message, Throwable cause) {
        super(message, cause);
    }
}
