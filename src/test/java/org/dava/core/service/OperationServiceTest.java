package org.dava.core.service;

import org.dava.common.StreamUtil;
import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.objects.database.structure.Table;
import org.dava.core.database.objects.dates.OffsetDate;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.MarshallingService;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.sql.objects.conditions.After;
import org.dava.core.sql.objects.conditions.Equals;
import org.dava.core.sql.objects.logic.operators.And;
import org.dava.external.Order;
import org.dava.common.Timer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.function.BiConsumer;

import static org.dava.common.StreamUtil.enumerate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationServiceTest {

    Database database;

    int ITERATIONS = 1000;

    void setUpWipeAndPopulate() throws IOException {
        if (FileUtil.exists("db/Order")) {
            FileUtil.deleteDirectory("db/Order");
        }

        setUpUseExisting();

        // make 1000 entries
        Random random = new Random();
        String tableName = "";
        for (int i = 0; i < ITERATIONS; i++) {
            OffsetDateTime time = OffsetDateTime.now();
            time = time.withYear(random.nextInt(2000, 2023));
            time = time.withMonth(random.nextInt(1, 13));
            time = time.withDayOfMonth(random.nextInt(1, 28));
            time = time.withHour(random.nextInt(0, 24));
            time = time.withMinute(random.nextInt(0, 60));
            time = time.withSecond(random.nextInt(0, 60));
            time = (random.nextBoolean())? time.withOffsetSameInstant(ZoneOffset.UTC) : time;
//            time = time.withOffsetSameInstant(ZoneOffset.UTC);
            Order orderTable = new Order(
                UUID.randomUUID().toString(),
                "This is a long description. A lot of tables could have something like this so this will be a good test.",
                BigDecimal.valueOf(Math.abs(random.nextLong(0, 100))),
                BigDecimal.valueOf(Math.abs(random.nextLong(0, 5))),
                time
            );
            Row row = MarshallingService.parseRow(orderTable).get(0);
            tableName = row.getTableName();
            BaseOperationService.insert(row, database, database.getTableByName(tableName), true);
        }

    }

    void setUpUseExisting() {
        database = new Database("db", List.of(Order.class));
    }


    @Test
    void insert() throws IOException {
        //   181,801 / 293,141 = 62% data, 28% meta
        //   193,031 / 254,167 = 76% data, 24% meta (with large text column)
        setUpWipeAndPopulate();

        Random random = new Random();

        Timer timer = Timer.start();
        String tableName = "";
        for (int i = 0; i < ITERATIONS; i++) {
            Order orderTable = new Order(
                UUID.randomUUID().toString(),
                "This is a long description. A lot of tables could have something like this so this will be a good test to include this.",
                BigDecimal.valueOf(Math.abs(random.nextLong(0, 100))),
                BigDecimal.valueOf(Math.abs(random.nextLong(0, 5))),
                OffsetDateTime.now());
            Row row = MarshallingService.parseRow(orderTable).get(0);
            tableName = row.getTableName();
            BaseOperationService.insert(row, database, database.getTableByName(tableName), true);
        }
        timer.printRestart();

        Table<?> table = database.getTableByName(tableName);
        long size = table.getSize(table.getRandomPartition());

        assertEquals(2 * ITERATIONS, size - 1);
    }

    @Test
    void equals() throws IOException {
        setUpWipeAndPopulate();

        Timer timer = Timer.start();
        Equals equals = new Equals("total", "25");
        List<Row> rows = equals.retrieve(database, new ArrayList<>(), "Order", 10L, 1L);
        timer.printRestart();

        rows.forEach( row ->
            assertEquals("25", row.getValue("total"))
        );
        assertTrue(rows.size() <= 10L);
    }

    @Test
    void and() throws IOException {
        setUpWipeAndPopulate();

        Timer timer = Timer.start();
        And and = new And(
            new Equals("total", "25"),
            new Equals("discount", "1")
        );
        List<Row> rows = and.retrieve(database, new ArrayList<>(), "Order", 10L, 1L);
        timer.printRestart();

        rows.forEach( row -> {
            assertEquals("25", row.getValue("total"));
            assertEquals("1", row.getValue("discount"));
        });
        assertTrue(rows.size() <= 10L);
        System.out.println(rows.size());
    }

    @Test
    void after() throws IOException {
        setUpUseExisting();

        long offset = 5L;

        Timer timer = Timer.start();
        After<OffsetDate> after = new After<>(
                "time",
                OffsetDate.of(OffsetDateTime.now().minusYears(10).toString()),
                true
        );
        List<Row> rows = after.retrieve(database, new ArrayList<>(), "Order", 10L, offset);
        timer.printRestart();

        StreamUtil.enumerate(rows, (i, row) -> {
            System.out.println( 1 + offset + i + " " + row.getValue("time") );
        });

//        Timer timer = Timer.start();
//        And and = new And(
//                new Equals("total", "25"),
//                new Equals("discount", "1")
//        );
//        List<Row> rows = and.retrieve(database, new ArrayList<>(), "Order", 10L, 1L);
//        timer.printRestart();
//
//        rows.forEach( row -> {
//            assertEquals("25", row.getValue("total"));
//            assertEquals("1", row.getValue("discount"));
//        });
//        assertTrue(rows.size() <= 10L);
//        System.out.println(rows.size());
    }

    @Test
    void test() {
        LocalDate localDate = LocalDate.now();
        LocalDateTime localDateTime = LocalDateTime.now();
        OffsetDateTime offsetDateTime = OffsetDateTime.now();
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        System.out.println(localDate);
        System.out.println(localDateTime);
        System.out.println(offsetDateTime);
        System.out.println(zonedDateTime);

        ZonedDateTime zt = LocalDate.of(2019, 10, 20)
            .atTime(1, 1, 1)
            .atZone(ZoneId.of("America/Denver"));

        OffsetDateTime unParsed = offsetDateTime.withOffsetSameInstant(ZoneOffset.MAX);
        System.out.println(unParsed);
        System.out.println(unParsed.isEqual(offsetDateTime));
    }

}
