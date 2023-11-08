package org.dava.core.service;

import org.dava.common.StreamUtil;
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
import org.dava.common.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.dava.common.Checks.safeCast;
import static org.dava.common.StreamUtil.enumerate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationServiceTest {

    static Database database;
    static int ITERATIONS = 1000;
    static Level logLevel = Level.DEBUG;
//    static String DB_ROOT = "/Users/brandon.fowler/Desktop/db";
    static Mode TABLE_MODE = Mode.MANUAL;
    static String DB_ROOT = "db";
    static Long seed = -183502108378805369L;
    private static final Logger log = Logger.getLogger(OperationServiceTest.class.getName());


    static void setUpWipeAndPopulate() throws IOException {
        if (FileUtil.exists(DB_ROOT + "/Order")) {
            FileUtil.deleteDirectory(DB_ROOT + "/Order");
        }

        seed = (seed == null)? new Random().nextLong() : seed;

        setUpUseExisting();

        Table<?> table = database.getTableByName("Order");
        List<Row> rows = makeRows(seed, ITERATIONS);

        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });

        Insert insert = new Insert(database, table, table.getRandomPartition());
        insert.insert(rows);

        log.info("Seed: " + seed);

    }

    static void setUpUseExisting() {
        database = new Database(DB_ROOT, List.of(Order.class), List.of(TABLE_MODE), seed);
    }

    static List<Row> makeRows(long seed, int iterations) {
        if (iterations <= 0)
            return new ArrayList<>();

        // make 1000 entries
        Random random = new Random(seed);

        return IntStream.range(0, iterations)
            .mapToObj( i -> {
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
                    UUID.nameUUIDFromBytes(String.valueOf(seed + i).getBytes()).toString(),
                    "This is a long description. A lot of tables could have something like this, so this will be a good test.",
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 100))),
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 5))),
                    time
                );
                return MarshallingService.parseRow(orderTable).get(0);
            })
            .toList();
    }

    @BeforeEach
    void setUp() throws IOException {
        Logger.setApplicationLogLevel(logLevel);
        setUpWipeAndPopulate();
//        setUpUseExisting(0L);
    }



    @Test
    void insert() {
        //   181,801 / 293,141 = 62% data, 28% meta
        //   193,031 / 254,167 = 76% data, 24% meta (with large text column)

        // in attractions the asset table had about 4-14 ms per insert
        // batch inserts are 14ms and don't increase much with a batch size <100
        // so for we're close but a little faster. 1ms for insert! though rollback logs may cost more time

        int INSERT_ITERATIONS = 1000;

        List<Row> rows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);

        Table<?> table = database.getTableByName("Order");

        rows.forEach(row -> {
            log.debug(Row.serialize(table, row.getColumnsToValues()));
        });

        Insert insert = new Insert(database, table, table.getRandomPartition());

        Timer timer = Timer.start();
        insert.insert(rows);
        timer.printRestart();

        long size = table.getSize(table.getRandomPartition());

        assertEquals(INSERT_ITERATIONS + ITERATIONS, size - 1);
    }

    @Test
    void equals() {
        Table<?> table = database.getTableByName("Order");

        Timer timer = Timer.start();
        Equals equals = new Equals("total", "25");
        List<Row> rows = equals.retrieve(table, new ArrayList<>(), 10L, null);
        timer.printRestart();

        rows.forEach( row -> {
            log.debug(
                Row.serialize(database.getTableByName(row.getTableName()), row.getColumnsToValues())
            );
            assertEquals("25", row.getValue("total"));
        });
        assertTrue(rows.size() <= 10L);
    }

    @Test
    void and() {
        Table<?> table = database.getTableByName("Order");

        Timer timer = Timer.start();
        And and = new And(
            new Equals("total", "25"),
            new Equals("discount", "1")
        );
        List<Row> rows = and.retrieve(table, new ArrayList<>(), 10L, 1L);
        timer.printRestart();

        rows.forEach( row -> {
            log.debug(
                Row.serialize(database.getTableByName(row.getTableName()), row.getColumnsToValues())
            );
            assertEquals("25", row.getValue("total"));
            assertEquals("1", row.getValue("discount"));
        });
        assertTrue(rows.size() <= 10L);
        log.debug(String.valueOf(rows.size()));
    }

    @Test
    void after() {
        Table<?> table = database.getTableByName("Order");

        long offset = 5L;
        OffsetDate date = OffsetDate.of(OffsetDateTime.now().minusYears(1).toString());

        Timer timer = Timer.start();
        After<OffsetDate> after = new After<>(
                "time",
                date,
                true
        );
        List<Row> rows = after.retrieve(table, new ArrayList<>(), 10L, offset);
        timer.printRestart();

        log.debug(date.toString());
        log.space();
        StreamUtil.enumerate(rows, (i, row) -> {
            log.debug( 1 + offset + i + " " + row.getValue("time") );
        });

        rows.forEach( row -> {
            assertTrue(
                    safeCast(row.getValue("time"), OffsetDate.class).isAfter(date)
            );
        });
    }

    @Test
    void before() {
        Table<?> table = database.getTableByName("Order");

        long offset = 5L;
        OffsetDate date = OffsetDate.of(OffsetDateTime.now().minusYears(10).toString());

        Timer timer = Timer.start();
        Before<OffsetDate> before = new Before<>(
                "time",
                date,
                true
        );
        List<Row> rows = before.retrieve(table, new ArrayList<>(), 10L, offset);
        timer.printRestart();

        log.debug(date.toString());
        log.space();
        StreamUtil.enumerate(rows, (i, row) -> {
            log.debug( 1 + offset + i + " " + row.getValue("time") );
        });

        rows.forEach( row -> {
            assertTrue(
                    safeCast(row.getValue("time"), OffsetDate.class).isBefore(date)
            );
        });
    }

    @Test
    void delete() {
        Table<?> table = database.getTableByName("Order");

        Equals equals = new Equals("total", "25");
        List<Row> rows = equals.retrieve(table, new ArrayList<>(), null, null);


        Delete delete = new Delete(database, table);
        Timer timer = Timer.start();
        delete.delete(rows);
        timer.printRestart();

        List<Row> afterRows = equals.retrieve(table, new ArrayList<>(), null, null);
        assertEquals(0, afterRows.size());
    }

    @Test
    void delete_and() {
        Table<?> table = database.getTableByName("Order");

        OffsetDate date = OffsetDate.of(OffsetDateTime.now().minusYears(10).toString());
        Before<OffsetDate> before = new Before<>(
            "time",
            date,
            true
        );
        List<Row> rows = before.retrieve(table, new ArrayList<>(), null, null);

        Delete delete = new Delete(database, table);
        delete.delete(rows);


        OffsetDate andDate = OffsetDate.of(OffsetDateTime.now().minusYears(1));
        And and = new And(
            new After<>("time", andDate, true),
            new Equals("discount", "1")
        );
        rows = and.retrieve(table, new ArrayList<>(), null, null);

        rows.forEach( row -> {
            log.debug(
                Row.serialize(database.getTableByName(row.getTableName()), row.getColumnsToValues())
            );
            assertEquals("1", row.getValue("discount"));
            OffsetDate rowDate = OffsetDate.of(row.getValue("time").toString());
            assertTrue(rowDate.isAfter(andDate) || rowDate.equals(andDate));
        });


    }





}
