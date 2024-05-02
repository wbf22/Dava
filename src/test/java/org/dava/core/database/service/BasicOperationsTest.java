package org.dava.core.database.service;

import org.dava.api.DavaId;
import org.dava.api.Order;
import org.dava.core.common.StreamUtil;
import org.dava.core.common.Timer;
import org.dava.core.common.logger.Level;
import org.dava.core.common.logger.Logger;
import org.dava.core.database.objects.dates.OffsetDate;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.operations.Delete;
import org.dava.core.database.service.operations.Insert;
import org.dava.core.database.service.structure.*;
import org.dava.core.sql.conditions.After;
import org.dava.core.sql.conditions.All;
import org.dava.core.sql.conditions.Before;
import org.dava.core.sql.conditions.Equals;
import org.dava.core.sql.conditions.GreaterThan;
import org.dava.core.sql.conditions.LessThan;
import org.dava.core.sql.operators.And;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.stream.IntStream;

import static org.dava.core.common.Checks.safeCast;
import static org.junit.jupiter.api.Assertions.*;

class BasicOperationsTest {

    static Database database;
    static int ITERATIONS = 1000;
    static Level logLevel = Level.DEBUG;
    // static Level logLevel = Level.INFO;

//    static String DB_ROOT = "/Users/brandon.fowler/Desktop/db";
    static Mode TABLE_MODE = Mode.INDEX_ALL;
    static String DB_ROOT = "db";
    static Long seed = -183502108378805369L;
    private static final Logger log = Logger.getLogger(BasicOperationsTest.class.getName());

    static FileUtil fileUtil = new FileUtil();

    static void setUpWipeAndPopulate() throws IOException {
        if (fileUtil.exists(DB_ROOT + "/Order")) {
            fileUtil.deleteDirectory(DB_ROOT + "/Order");
        }

        seed = (seed == null)? new Random().nextLong() : seed;

        setUpUseExisting();

        Table<?> table = database.getTableByName("Order");
        List<Row> rows = makeRows(seed, ITERATIONS);

        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });

        Insert insert = new Insert(database, table, table.getRandomPartition());
        insert.insert(rows, true);

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
                OffsetDateTime time = OffsetDateTime.of(
                    random.nextInt(2000, 2023),
                    random.nextInt(1, 13),
                    random.nextInt(1, 28),
                    random.nextInt(0, 24),
                    random.nextInt(0, 60),
                    random.nextInt(0, 60),
                    0,
                    (random.nextBoolean())? ZoneOffset.UTC : ZoneOffset.MAX
                );
//            time = time.withOffsetSameInstant(ZoneOffset.UTC);
                Order orderTable = new Order(
                    DavaId.generateId("order", String.valueOf(random.nextInt(1000000000))),
                    "This is a long description. A lot of tables could have something like this, so this will be a good test.",
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 100))),
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 5))),
                    time
                );
                Map<String, List<Row>> res = MarshallingService.parseRow(orderTable);
                return res.get(orderTable.getClass().getSimpleName()).get(0);
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
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });

        Insert insert = new Insert(database, table, table.getRandomPartition());

        Timer timer = Timer.start();
        insert.insert(rows, true);
        timer.printRestart();

        long size = table.getSize(table.getRandomPartition());

        assertEquals(INSERT_ITERATIONS + ITERATIONS, size - 1);
    }

    @Test
    void equals() {
        Table<?> table = database.getTableByName("Order");

        Timer timer = Timer.start();
        Equals equals = new Equals("total", "25");
        List<Row> rows = equals.retrieve(table, new ArrayList<>(), 10, null);
        timer.printRestart();

        rows.forEach( row -> {
            log.debug(
                Row.serialize(database.getTableByName(row.getTableName()), row.getColumnsToValues())
            );
            assertEquals(BigDecimal.valueOf(25), row.getValue("total"));
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
        List<Row> rows = and.retrieve(table, new ArrayList<>(), 10, 1L);
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
        OffsetDate date = OffsetDate.of(OffsetDateTime.now().minusYears(3).toString());

        Timer timer = Timer.start();
        After<OffsetDate> after = new After<>(
                "time",
                date,
                true
        );
        List<Row> rows = after.retrieve(table, new ArrayList<>(), 10, offset);
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
        List<Row> rows = before.retrieve(table, new ArrayList<>(), 10, offset);
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
        delete.delete(rows, true);
        timer.printRestart();

        List<Row> afterRows = equals.retrieve(table, new ArrayList<>(), null, null);
        assertEquals(0, afterRows.size());
    }

    @Test
    void rollback_insert() {
        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);


        // do insert
        int INSERT_ITERATIONS = 100;
        List<Row> rows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);
        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert = new Insert(database, table, partition);
        insert.insert(rows, true);

        // get all rows
        List<Row> allRowBefore = new All().retrieve(table, List.of(), null, null);

        // rollback
        Timer timer = Timer.start();
        Rollback rollback = new Rollback();
        rollback.rollback(table, partition, table.getRollbackPath(partition));
        timer.printRestart();

        // get all rows
        List<Row> allRowAfter = new All().retrieve(table, List.of(), null, null);


        // assert table is the same size after rollback
        long size = table.getSize(partition);
        assertEquals(intialSize, size);

        // assert none of the insert rows are in the table
        allRowAfter.forEach(afterRow -> {
            rows.forEach(insertRow -> {
                assertNotEquals(insertRow, afterRow);
            });
        });
    }

    @Test
    void rollback_delete() {
        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);

        // get all rows
        List<Row> allRowBefore = new All().retrieve(table, List.of(), null, null);

        // do delete
        Equals equals = new Equals("discount", "1");
        List<Row> rows = equals.retrieve(table, new ArrayList<>(), null, null);
        Delete delete = new Delete(database, table);
        delete.delete(rows, true);

        // rollback
        Timer timer = Timer.start();
        Rollback rollback = new Rollback();
        rollback.rollback(table, partition, table.getRollbackPath(partition));
        timer.printRestart();
//
//        // get all rows
//        List<Row> allRowAfter = new All().retrieve(table, List.of(), null, null);
//
//        // assert table is the same size after rollback
//        long size = table.getSize(partition);
//        assertEquals(intialSize, size);
//
//        // assert all the rows before are in all rows after
//        allRowAfter.forEach(afterRow -> {
//            long count = allRowBefore.stream().filter(beforeRow -> beforeRow.equals(afterRow)).count();
//            assertEquals(1L, count);
//        });
    }



    // rollback halfway through transactions
    // rollback with using empties
    // delete, insert using empties
    //


    @Test
    void repetitive_reads() {
        Table<?> table = database.getTableByName("Order");
        OffsetDate date = OffsetDate.of(OffsetDateTime.now().minusYears(10).toString());
        Before<OffsetDate> before = new Before<>(
            "time",
            date,
            true
        );

        // warm up
        for (int i = 0; i < 10; i++) {
            List<Row> rows = before.retrieve(table, new ArrayList<>(), null, null);
        }

        Timer timer;

        // without using cache
        timer = Timer.start();
        for (int i = 0; i < 10; i++) {
            List<Row> rows = before.retrieve(table, new ArrayList<>(), null, null);
        }
        timer.printRestart();


        assertTrue(true);
    }


    @Test
    void greaterThan() {
        Table<?> table = database.getTableByName("Order");

        long offset = 5L;
        BigDecimal value = BigDecimal.valueOf(10);

        Timer timer = Timer.start();
        GreaterThan greaterThan = new GreaterThan(
            "total",
            value,
            true,
            null
        );
        List<Row> rows = greaterThan.retrieve(table, new ArrayList<>(), 10, offset);
        timer.printRestart();

        log.debug(value.toString());
        log.space();
        StreamUtil.enumerate(rows, (i, row) -> {
            log.debug( row.toStringExcludeColumns(Set.of("description")) );
        });

        rows.forEach( row -> {
            assertTrue(
                    safeCast(row.getValue("total"), BigDecimal.class).compareTo(value) > 0
            );
        });
    }



    @Test
    void lessThan() {
        Table<?> table = database.getTableByName("Order");

        long offset = 0L;
        BigDecimal value = BigDecimal.valueOf(10);

        Timer timer = Timer.start();
        LessThan lessThan = new LessThan(
                "total",
                value,
                true,
                null
        );
        List<Row> rows = lessThan.retrieve(table, new ArrayList<>(), 10, offset);
        timer.printRestart();

        log.debug(value.toString());
        log.space();
        StreamUtil.enumerate(rows, (i, row) -> {
            log.debug( row.toStringExcludeColumns(Set.of("description")) );
        });

        rows.forEach( row -> {
            assertTrue(
                    safeCast(row.getValue("total"), BigDecimal.class).compareTo(value) < 0
            );
        });
    }


}
