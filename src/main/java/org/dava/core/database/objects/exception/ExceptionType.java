package org.dava.core.database.objects.exception;

public enum ExceptionType {
    CORRUPTED_ROW_ERROR,
    INDEX_READ_ERROR,
    ROW_MISSING_PUBLIC_GETTER,
    NOT_A_TABLE,
    MISSING_TABLE,
    REPOSITORY_ERROR,
    BASE_IO_ERROR;
}
