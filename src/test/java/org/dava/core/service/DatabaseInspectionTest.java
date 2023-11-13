package org.dava.core.service;

import org.dava.common.StreamUtil;
import org.dava.common.Timer;
import org.dava.common.logger.Level;
import org.dava.common.logger.Logger;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.dates.OffsetDate;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.Delete;
import org.dava.core.database.service.Insert;
import org.dava.core.database.service.MarshallingService;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.sql.objects.conditions.After;
import org.dava.core.sql.objects.conditions.Before;
import org.dava.core.sql.objects.conditions.Equals;
import org.dava.core.sql.objects.logic.operators.And;
import org.dava.external.Order;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.dava.common.Checks.safeCast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseInspectionTest {

    static Database database;
    static int ITERATIONS = 1000;
    static Level logLevel = Level.INFO;
//    static String DB_ROOT = "/Users/brandon.fowler/Desktop/db";
    static Mode TABLE_MODE = Mode.MANUAL;
    static String DB_ROOT = "db";
    static Long seed = -183502108378805369L;
    private static final Logger log = Logger.getLogger(DatabaseInspectionTest.class.getName());


    static void setUpUseExisting() {
        database = new Database(DB_ROOT, List.of(Order.class), List.of(TABLE_MODE), seed);
    }
    @BeforeEach
    void setUp() throws IOException {
        Logger.setApplicationLogLevel(logLevel);
        setUpUseExisting();
    }



    @Test
    void getIndices() {
        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();

        String indexPath = "db/Order/indecis_Order/total/0.index";
        List<IndexRoute> indices = BaseOperationService.getRoutes(indexPath, partition, 8L, null).getSecond();

        indices.forEach( route -> {
            log.debug(route.toString());

            String row = String.join(
                "\n",
                BaseOperationService.getLinesUsingRoutes(partition, table, List.of(route))
            );
            log.debug(row);
        });

        log.debug("Total Rows: " + indices.size());
    }

    @Test
    void getEmtpies() throws IOException {
        IndexRoute route = new IndexRoute(null, 5289L, 178);
        byte[] bytes = route.getRouteAsBytes();
        IndexRoute parsed = IndexRoute.parseRoute(bytes, null);


        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();

        String empties = BaseOperationService.getAllEmpties(table.emptiesFilePath(partition)).stream()
            .map(Object::toString)
            .collect(Collectors.joining());
        log.info(empties);


    }

    @Test
    void test() {
        LocalDate localDate = LocalDate.now();
        LocalDateTime localDateTime = LocalDateTime.now();
        OffsetDateTime offsetDateTime = OffsetDateTime.now();
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        log.debug(localDate.toString());
        log.debug(localDateTime.toString());
        log.debug(offsetDateTime.toString());
        log.debug(zonedDateTime.toString());

        ZonedDateTime zt = LocalDate.of(2019, 10, 20)
            .atTime(1, 1, 1)
            .atZone(ZoneId.of("America/Denver"));

        OffsetDateTime unParsed = offsetDateTime.withOffsetSameInstant(ZoneOffset.MAX);
        log.debug(unParsed.toString());
        log.debug(String.valueOf(unParsed.isEqual(offsetDateTime)));

        log.space();
        ZonedDateTime newYears = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.MAX);
        ZonedDateTime notYetNewYear = newYears.plusMinutes(1).withZoneSameInstant(ZoneOffset.MIN);
        log.debug(String.valueOf(notYetNewYear.isAfter(newYears)));

        log.debug("New years: " + newYears);
        log.debug("Same but min time zone: " + notYetNewYear);
        log.debug("Converted back: " + notYetNewYear.withZoneSameInstant(ZoneOffset.MAX));


        BigDecimal bdI = new BigDecimal( Integer.parseInt("0") );
        BigDecimal bdL = new BigDecimal( Long.parseLong("0") );
        BigDecimal bdD = new BigDecimal( Double.parseDouble("0.31") );
        BigDecimal bdD2 = BigDecimal.valueOf( Double.parseDouble("0.31") );
        BigDecimal bdF = new BigDecimal( Float.parseFloat("48984.2354") );

        log.debug(bdI.toString());
        log.debug(bdL.toString());
        log.debug(bdD.toString());
        log.debug(bdD2.toString());
        log.debug(bdF.toString());

    }




}
