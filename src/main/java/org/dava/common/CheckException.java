package org.dava.common;

public class CheckException extends RuntimeException {
    public CheckException(String message, Throwable cause) {
        super(message, cause);
    }
}
