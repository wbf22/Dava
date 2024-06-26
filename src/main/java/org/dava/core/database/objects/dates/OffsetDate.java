package org.dava.core.database.objects.dates;

import org.dava.core.database.objects.exception.DavaException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.dava.core.database.objects.exception.ExceptionType.DATE_PARSE_ERROR;

public class OffsetDate extends Date<OffsetDateTime> {

    private final OffsetDateTime offsetDateTime;

    public OffsetDate(Integer year, String stringValue, OffsetDateTime offsetDateTime) {
        this.year = year;
        this.stringValue = stringValue;
        this.offsetDateTime = offsetDateTime;
        this.type = OffsetDateTime.class;
    }


    public static OffsetDate of(String stringValue) {
        OffsetDateTime parsed = OffsetDateTime.parse(stringValue);
        return new OffsetDate(
                parsed.withOffsetSameInstant(ZoneOffset.UTC).getYear(),
                stringValue,
                parsed
        );
    }

    public static OffsetDate of(OffsetDateTime date) {
        return new OffsetDate(
                date.withOffsetSameInstant(ZoneOffset.UTC).getYear(),
                date.toString(),
                date
        );
    }


    @Override
    public boolean isAfter(Date<?> date) {
        if (date instanceof OffsetDate bDate)
            return offsetDateTime.isAfter(bDate.offsetDateTime);

        return getDateWithoutTime().isAfter(date.getDateWithoutTime());
    }

    @Override
    public boolean isBefore(Date<?> date) {
        if (date instanceof OffsetDate bDate)
            return offsetDateTime.isBefore(bDate.offsetDateTime);

        return getDateWithoutTime().isBefore(date.getDateWithoutTime());
    }

    @Override
    public boolean isBetween(Date<?> start, Date<?> end) {
        if (start instanceof OffsetDate startDate && (end instanceof OffsetDate endDate))
            return offsetDateTime.isAfter(startDate.offsetDateTime) && offsetDateTime.isBefore(endDate.offsetDateTime);

        LocalDate localDate = getDateWithoutTime();
        return localDate.isAfter(start.getDateWithoutTime()) && localDate.isBefore(end.getDateWithoutTime());
    }

    @Override
    public LocalDate getDateWithoutTime() {
        return offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC).toLocalDate();
    }


    @Override
    public int compareTo(Date<OffsetDateTime> other) {
        if (other instanceof OffsetDate otherOffset) {
            return this.offsetDateTime.compareTo(otherOffset.offsetDateTime);
        }
        return this.getDateWithoutTime().compareTo(other.getDateWithoutTime());
    }


    @Override
    public Long getHoursSinceEpoch() {
        return this.offsetDateTime.toEpochSecond() / SECONDS_IN_HOUR;
    }


    @Override
    public Instant getInstant() {
        return this.offsetDateTime.toInstant();
    }
}
