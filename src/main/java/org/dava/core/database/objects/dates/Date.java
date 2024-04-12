package org.dava.core.database.objects.dates;

import org.dava.core.database.objects.exception.DavaException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.*;
import java.util.Objects;

import static org.dava.core.database.objects.exception.ExceptionType.DATE_PARSE_ERROR;

public abstract class Date<T> implements Comparable<Date<T>> {
    //  ZonedDateTime (most info): 2023-10-13T07:21:57.703778-06:00[America/Denver]
    protected Integer year;
    protected String stringValue;
    protected Class<T> type;

    public static final long SECONDS_IN_HOUR = 3600;



    public abstract boolean isAfter(Date<?> date);

    public abstract boolean isBefore(Date<?> date);

    public abstract boolean isBetween(Date<?> start, Date<?> end);

    public abstract Long getHoursSinceEpoch();

    public abstract Instant getInstant();

    public abstract LocalDate getDateWithoutTime();




    public static Date<?> ofForDava(String stringValue, Class<?> davaDateType) {
        Exception error = null;

        try {
            if (davaDateType == BasicDate.class) {
                return BasicDate.of(stringValue);
            }
            else if (davaDateType == BasicDateTime.class) {
                return BasicDateTime.of(stringValue);
            }
            else if (davaDateType == OffsetDate.class) {
                return OffsetDate.of(stringValue);
            }
            else if (davaDateType == ZonedDate.class) {
                return ZonedDate.of(stringValue);
            }
        } catch (RuntimeException e) {
            error = e;
        }

        throw new DavaException(
            DATE_PARSE_ERROR,
            "Tried to parse date from bad string: '" + stringValue + "' or unsupported type: '" + davaDateType.getName() + "'",
            error
        );
    }

    public static Date<?> of(String stringValue, Class<?> javaDateType) {
        Exception error = null;

        try {
            if (javaDateType == LocalDate.class) {
                return BasicDate.of(stringValue);
            }
            else if (javaDateType == LocalDateTime.class) {
                return BasicDateTime.of(stringValue);
            }
            else if (javaDateType == OffsetDateTime.class) {
                return OffsetDate.of(stringValue);
            }
            else if (javaDateType == ZonedDateTime.class) {
                return ZonedDate.of(stringValue);
            }
        } catch (RuntimeException e) {
            error = e;
        }

        throw new DavaException(
            DATE_PARSE_ERROR,
            "Tried to parse date from bad string: '" + stringValue + "' or unsupported type: '" + javaDateType.getName() + "'",
            error
        );
    }

    public static Date<?> ofOrLocalDateOnFailure(String stringValue, Class<?> javaDateType) {
        // TODO parsing Strings to Dates is actually a bit of a slow down. Consider parsing manually to see if its faster
        // run OperationServiceTest.repetitive_reads() with profile to see this

        Exception error = null;

        try {
            if (javaDateType == LocalDate.class) {
                return BasicDate.of(stringValue);
            }
            else if (javaDateType == LocalDateTime.class) {
                return BasicDateTime.of(stringValue);
            }
            else if (javaDateType == OffsetDateTime.class) {
                return OffsetDate.of(stringValue);
            }
            else if (javaDateType == ZonedDateTime.class) {
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
            "Tried to parse date from bad string: '" + stringValue + "' or unsupported type: '" + javaDateType.getName() + "'",
            error
        );
    }

    public static boolean isDateSupportedDateType(Class<?> type) {
        return type == BasicDate.class || type == LocalDateTime.class || type == OffsetDateTime.class || type == ZonedDateTime.class;
    }

    /**
     * Returns the milliseconds since the epoch with decimal precision (partial milliseconds)
     * @return
     */
    public BigDecimal getMillisecondsSinceTheEpoch() {
        Instant instant = getInstant();
        BigDecimal seconds = BigDecimal.valueOf(instant.getEpochSecond());
        BigDecimal nanoseconds = BigDecimal.valueOf(instant.getNano());
        return seconds.multiply(BigDecimal.valueOf(1000)).add(nanoseconds.divide(BigDecimal.valueOf(1_000_000)));
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
