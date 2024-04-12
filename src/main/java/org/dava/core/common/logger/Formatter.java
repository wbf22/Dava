package org.dava.core.common.logger;

import static org.dava.core.common.logger.AnsiColor.*;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public interface Formatter {


    String format(String message);


    default String getTimeStampIntellij() {
        ZonedDateTime date = ZonedDateTime.now();
        return BLACK_HIGH_INTENSITY.BOLD() + date.format(
                DateTimeFormatter.ofPattern("yyyy-MM-dd ")
        ) + WHITE.DIM() + date.format(
                DateTimeFormatter.ofPattern("HH:mm ")
        ) + BLACK_HIGH_INTENSITY + date.format(
                DateTimeFormatter.ofPattern("ss.SS")
        )  + "s " + date.format(
                DateTimeFormatter.ofPattern("xx ")
        ) + WHITE.DIM() + date.format(
                DateTimeFormatter.ofPattern("'['VV']'")
        ) + RESET;
    }


    default String getTimeStamp() {
        ZonedDateTime date = ZonedDateTime.now();
        return WHITE.ITALIC() + date.format(
                DateTimeFormatter.ofPattern("yyyy-MM-dd ")
        ) + WHITE.DIM() + date.format(
                DateTimeFormatter.ofPattern("HH:mm ")
        ) + WHITE.ITALIC() + date.format(
                DateTimeFormatter.ofPattern("ss.SS")
        )  + "s " + date.format(
                DateTimeFormatter.ofPattern("xx ")
        ) + WHITE.DIM() + date.format(
                DateTimeFormatter.ofPattern("'['VV']'")
        ) + RESET;
    }

}
