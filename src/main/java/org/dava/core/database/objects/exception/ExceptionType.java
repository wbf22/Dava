package org.dava.core.database.objects.exception;

public enum ExceptionType {
    DATE_PARSE_ERROR,
    CORRUPTED_ROW_ERROR,
    UNIQUE_CONSTRAINT_VIOLATION,
    ROLLBACK_ERROR,
    INDEX_CREATION_ERROR,
    INDEX_READ_ERROR,
    ROW_MISSING_PUBLIC_GETTER,
    TABLE_PARSE_ERROR,
    NOT_A_TABLE,
    MISSING_TABLE,
    REPOSITORY_ERROR,
    BASE_IO_ERROR,
    LIMIT_ERROR,
    CACHE_ERROR
}
