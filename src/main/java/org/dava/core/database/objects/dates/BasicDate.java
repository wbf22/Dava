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

        throw new DavaException(
                DATE_PARSE_ERROR,
                "Incorrect date type: '" + date.getType().getSimpleName() + "'",
                null
        );
    }

    @Override
    public boolean isBefore(Date<?> date) {
        if (date instanceof BasicDate bDate)
            return localDate.isBefore(bDate.localDate);

        throw new DavaException(
                DATE_PARSE_ERROR,
                "Incorrect date type: '" + date.getType().getSimpleName() + "'",
                null
        );
    }

    @Override
    public boolean isBetween(Date<?> start, Date<?> end) {
        if (start instanceof BasicDate startDate && (end instanceof BasicDate endDate))
            return localDate.isAfter(startDate.localDate) && localDate.isBefore(endDate.localDate);

        throw new DavaException(
                DATE_PARSE_ERROR,
                "Incorrect date type(s) in: '" + start.getType().getSimpleName() + "' '" + end.getType().getSimpleName() + "'",
                null
        );
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
}
