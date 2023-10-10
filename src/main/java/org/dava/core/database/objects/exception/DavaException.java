package org.dava.core.database.objects.exception;

public class DavaException extends RuntimeException{
    public DavaException(ExceptionType type, String message, Throwable cause) {
        super(type.name() + " " + message, cause);
    }
}
