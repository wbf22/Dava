package org.dava.core.database.objects.dates;

import org.dava.core.database.objects.exception.DavaException;

import java.io.Serializable;
import java.time.*;
import java.util.Objects;

import static org.dava.core.database.objects.exception.ExceptionType.DATE_PARSE_ERROR;

public abstract class Date<T> implements Comparable<Date<T>> {
    //  ZonedDateTime (most info): 2023-10-13T07:21:57.703778-06:00[America/Denver]
    protected Integer year;
    protected String stringValue;
    protected Class<T> type;



    public abstract boolean isAfter(Date<?> date);

    public abstract boolean isBefore(Date<?> date);

    public abstract boolean isBetween(Date<?> start, Date<?> end);

    public abstract LocalDate getDateWithoutTime();





    public static Date<?> of(String stringValue, Class<?> type) {
        Exception error = null;

        try {
            if (type == LocalDate.class) {
                return BasicDate.of(stringValue);
            }
            else if (type == LocalDateTime.class) {
                return BasicDateTime.of(stringValue);
            }
            else if (type == OffsetDateTime.class) {
                return OffsetDate.of(stringValue);
            }
            else if (type == ZonedDateTime.class) {
                return ZonedDate.of(stringValue);
            }
        } catch (RuntimeException e) {
            error = e;
        }

        throw new DavaException(
            DATE_PARSE_ERROR,
            "Tried to parse date from bad string: '" + stringValue + "' or unsupported type: '" + type.getName() + "'",
            error
        );
    }

    public static Date<?> ofOrLocalDateOnFailure(String stringValue, Class<?> type) {
        Exception error = null;

        try {
            if (type == LocalDate.class) {
                return BasicDate.of(stringValue);
            }
            else if (type == LocalDateTime.class) {
                return BasicDateTime.of(stringValue);
            }
            else if (type == OffsetDateTime.class) {
                return OffsetDate.of(stringValue);
            }
            else if (type == ZonedDateTime.class) {
                return ZonedDate.of(stringValue);
            }
        } catch (RuntimeException e) {
            try {
                return BasicDate.of(stringValue);
            } catch (RuntimeException e2) {
                error = e2;
            }
        }


        throw new DavaException(
            DATE_PARSE_ERROR,
            "Tried to parse date from bad string: '" + stringValue + "' or unsupported type: '" + type.getName() + "'",
            error
        );
    }

    public static boolean isDateSupportedDateType(Class<?> type) {
        return type == BasicDate.class || type == LocalDateTime.class || type == OffsetDateTime.class || type == ZonedDateTime.class;
    }



    @Override
    public String toString() {
        return stringValue;
    }



    /*
        getter setter
     */

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }


    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    public Class<T> getType() {
        return type;
    }

    public void setType(Class<T> type) {
        this.type = type;
    }


    public boolean equals(Date<?> date) {
        if (this == date) return true;
        return stringValue.equals(date.stringValue);
    }

}
