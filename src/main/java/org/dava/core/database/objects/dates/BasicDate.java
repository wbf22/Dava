package org.dava.core.database.objects.dates;

import org.dava.core.database.objects.exception.DavaException;

import java.time.*;

import static org.dava.core.database.objects.exception.ExceptionType.DATE_PARSE_ERROR;

public class BasicDate extends Date<LocalDate> {

    private final LocalDate localDate;

    public BasicDate(Integer year, String stringValue, LocalDate localDate) {
        this.year = year;
        this.stringValue = stringValue;
        this.localDate = localDate;
        this.type = LocalDate.class;
    }


    public static BasicDate of(String stringValue) {
        LocalDate parsed = LocalDate.parse(stringValue);
        return new BasicDate(
                parsed.getYear(),
                stringValue,
                parsed
        );
    }

    @Override
    public boolean isAfter(Date<?> date) {
        if (date instanceof BasicDate bDate)
            return localDate.isAfter(bDate.localDate);

        return localDate.isAfter(date.getDateWithoutTime());
    }

    @Override
    public boolean isBefore(Date<?> date) {
        if (date instanceof BasicDate bDate)
            return localDate.isBefore(bDate.localDate);

        return localDate.isBefore(date.getDateWithoutTime());
    }

    @Override
    public boolean isBetween(Date<?> start, Date<?> end) {
        if (start instanceof BasicDate startDate && (end instanceof BasicDate endDate))
            return localDate.isAfter(startDate.localDate) && localDate.isBefore(endDate.localDate);

        LocalDate localDate = getDateWithoutTime();
        return localDate.isAfter(start.getDateWithoutTime()) && localDate.isBefore(end.getDateWithoutTime());
    }

    @Override
    public LocalDate getDateWithoutTime() {
        return localDate;
    }

    @Override
    public int compareTo(Date<LocalDate> other) {
        if (other instanceof BasicDate otherBasic) {
            return this.localDate.compareTo(otherBasic.localDate);
        }
        return this.getDateWithoutTime().compareTo(other.getDateWithoutTime());
    }


    @Override
    public Long getHoursSinceEpoch() {
        return this.localDate.atStartOfDay(ZoneOffset.UTC).toEpochSecond() / SECONDS_IN_HOUR;
    }
}
