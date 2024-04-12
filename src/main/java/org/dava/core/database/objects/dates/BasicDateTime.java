package org.dava.core.database.objects.dates;

import org.dava.core.database.objects.exception.DavaException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.dava.core.database.objects.exception.ExceptionType.DATE_PARSE_ERROR;

public class BasicDateTime extends Date<LocalDateTime> {

    private final LocalDateTime localDateTime;

    public BasicDateTime(Integer year, String stringValue, LocalDateTime localDateTime) {
        this.year = year;
        this.stringValue = stringValue;
        this.localDateTime = localDateTime;
        this.type = LocalDateTime.class;
    }


    public static BasicDateTime of(String stringValue) {
        LocalDateTime parsed = LocalDateTime.parse(stringValue);
        return new BasicDateTime(
                parsed.getYear(),
                stringValue,
                parsed
        );
    }

    @Override
    public boolean isAfter(Date<?> date) {
        if (date instanceof BasicDateTime bDate)
            return localDateTime.isAfter(bDate.localDateTime);

        return getDateWithoutTime().isAfter(date.getDateWithoutTime());
    }

    @Override
    public boolean isBefore(Date<?> date) {
        if (date instanceof BasicDateTime bDate)
            return localDateTime.isBefore(bDate.localDateTime);

        return getDateWithoutTime().isBefore(date.getDateWithoutTime());
    }

    @Override
    public boolean isBetween(Date<?> start, Date<?> end) {
        if (start instanceof BasicDateTime startDate && (end instanceof BasicDateTime endDate))
                return localDateTime.isAfter(startDate.localDateTime) && localDateTime.isBefore(endDate.localDateTime);

        LocalDate localDate = getDateWithoutTime();
        return localDate.isAfter(start.getDateWithoutTime()) && localDate.isBefore(end.getDateWithoutTime());
    }


    @Override
    public LocalDate getDateWithoutTime() {
        return localDateTime.toLocalDate();
    }


    @Override
    public int compareTo(Date<LocalDateTime> other) {
        if (other instanceof BasicDateTime otherBasic) {
            return this.localDateTime.compareTo(otherBasic.localDateTime);
        }
        return this.getDateWithoutTime().compareTo(other.getDateWithoutTime());
    }


    @Override
    public Long getHoursSinceEpoch() {
        return this.localDateTime.toEpochSecond(ZoneOffset.UTC) / SECONDS_IN_HOUR;
    }


    @Override
    public Instant getInstant() {
        return this.localDateTime.toInstant(ZoneOffset.UTC);
    }
}
