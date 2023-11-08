package org.dava.core.database.objects.dates;

import org.dava.core.database.objects.exception.DavaException;

import java.time.*;

import static org.dava.core.database.objects.exception.ExceptionType.DATE_PARSE_ERROR;

public class ZonedDate extends Date<ZonedDateTime> {

    private final ZonedDateTime zonedDateTime;

    public ZonedDate(Integer year, String stringValue, ZonedDateTime zonedDateTime) {
        this.year = year;
        this.stringValue = stringValue;
        this.zonedDateTime = zonedDateTime;
        this.type = ZonedDateTime.class;
    }


    public static ZonedDate of(String stringValue) {
        ZonedDateTime parsed = ZonedDateTime.parse(stringValue);
        return new ZonedDate(
                parsed.withZoneSameInstant(ZoneId.of("UTC")).getYear(),
                stringValue,
                parsed
        );
    }


    @Override
    public boolean isAfter(Date<?> date) {
        if (date instanceof ZonedDate bDate)
            return zonedDateTime.isAfter(bDate.zonedDateTime);

        return getDateWithoutTime().isAfter(date.getDateWithoutTime());
    }

    @Override
    public boolean isBefore(Date<?> date) {
        if (date instanceof ZonedDate bDate)
            return zonedDateTime.isBefore(bDate.zonedDateTime);

        return getDateWithoutTime().isBefore(date.getDateWithoutTime());
    }

    @Override
    public boolean isBetween(Date<?> start, Date<?> end) {
        if (start instanceof ZonedDate startDate && (end instanceof ZonedDate endDate))
            return zonedDateTime.isAfter(startDate.zonedDateTime) && zonedDateTime.isBefore(endDate.zonedDateTime);

        LocalDate localDate = getDateWithoutTime();
        return localDate.isAfter(start.getDateWithoutTime()) && localDate.isBefore(end.getDateWithoutTime());
    }

    @Override
    public LocalDate getDateWithoutTime() {
        return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")).toLocalDate();
    }


    @Override
    public int compareTo(Date<ZonedDateTime> other) {
        if (other instanceof ZonedDate otherZoned) {
            return this.zonedDateTime.compareTo(otherZoned.zonedDateTime);
        }
        return this.getDateWithoutTime().compareTo(other.getDateWithoutTime());
    }
}
