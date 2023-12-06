package org.dava.core.database.service;

import org.dava.common.Timer;
import org.dava.common.logger.Level;
import org.dava.common.logger.Logger;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.dates.OffsetDate;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.Cache;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.sql.objects.conditions.After;
import org.dava.core.sql.objects.conditions.All;
import org.dava.core.sql.objects.conditions.Before;
import org.dava.core.sql.objects.conditions.Equals;
import org.dava.core.sql.objects.logic.operators.And;
import org.dava.external.DavaTSID;
import org.dava.external.Order;
import org.dava.core.database.service.BaseOperationService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;


class HardOperationsTest {

    static Database database;
    static int ITERATIONS = 1000;
    static Level logLevel = Level.INFO;
//    static String DB_ROOT = "/Users/brandon.fowler/Desktop/db";
    static String DB_ROOT = "db";
    static Long seed = -183502108378805369L;
    private static final Logger log = Logger.getLogger(HardOperationsTest.class.getName());


    static void setUpWipeAndPopulate(Mode tableMode) throws IOException {
        if (FileUtil.exists(DB_ROOT + "/Order")) {
            FileUtil.deleteDirectory(DB_ROOT + "/Order");
        }

        seed = (seed == null)? new Random().nextLong() : seed;

        setUpUseExisting(tableMode);

        Table<?> table = database.getTableByName("Order");
        List<Row> rows = makeRows(seed, ITERATIONS);

        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });

        Insert insert = new Insert(database, table, table.getRandomPartition());
        insert.insert(rows);

        log.info("Seed: " + seed);

    }

    static void setUpUseExisting(Mode tableMode) {
        database = new Database(DB_ROOT, List.of(Order.class), List.of(tableMode), seed);
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
                    DavaTSID.generateId("order", String.valueOf(random.nextInt(1000000000))),
                    "This is a long description. A lot of tables could have something like this, so this will be a good test.",
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 100))),
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 5))),
                    time
                );
                return MarshallingService.parseRow(orderTable).get(0);
            })
            .toList();
    }

    void setUp(Mode tableMode) throws IOException {

        Logger.setApplicationLogLevel(logLevel);
        setUpWipeAndPopulate(tableMode);
//        setUpUseExisting(0L);
    }


    static int oldNumericPartitionSize = BaseOperationService.NUMERIC_PARTITION_SIZE;
    @BeforeAll
    static void setNumericPartitionSize() {
        BaseOperationService.NUMERIC_PARTITION_SIZE = 10;
    }
    @AfterAll
    static void resetNumericPartitionSize() {
        BaseOperationService.NUMERIC_PARTITION_SIZE = oldNumericPartitionSize;
    }


    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    void delete_and(Mode tableMode) throws IOException {
        // order_86B122D 120988, 154
        setUp(tableMode);

        Table<?> table = database.getTableByName("Order");

        OffsetDate date = OffsetDate.of(OffsetDateTime.now().minusYears(10).toString());
        Before<OffsetDate> before = new Before<>(
            "time",
            date,
            true
        );
        List<Row> rows = before.retrieve(table, new ArrayList<>(), null, null);

        List<String> lines = BaseOperationService.getFileSizeAndRoutes(
                "db/Order/META_Order/discount/4.index",
                "Order",
                0L,
                null
            ).getSecond().stream()
            .map(route -> {
                try {
                    return new String(
                        FileUtil.readBytes(table.getTablePath("Order"), route.getOffsetInTable(), route.getLengthInTable()),
                        StandardCharsets.UTF_8
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .filter(line -> line.contains("order_86B122D"))
            .toList();


        Delete delete = new Delete(database, table);
        delete.delete(rows);

        List<String> x = BaseOperationService.getFileSizeAndRoutes(
                "db/Order/META_Order/discount/4.index",
                "Order",
                0L,
                null
            ).getSecond().stream()
            .map(route -> {
                try {
                    return new String(
                        FileUtil.readBytes(table.getTablePath("Order"), route.getOffsetInTable(), route.getLengthInTable()),
                        StandardCharsets.UTF_8
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .filter(line -> line.contains("order_86B122D"))
            .toList();

        Equals equals = new Equals("discount", "4");
        List<Row> postDeleteRows = equals.retrieve(table, new ArrayList<>(), null, null);
        postDeleteRows.forEach( row -> {
            OffsetDate rowDate = OffsetDate.of(row.getValue("time").toString());
            rowDate.isAfter(date);
        });

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

    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    void rollback_insert_failure_during_addToTable(Mode tableMode) throws IOException {

        setUp(tableMode);

        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);


        // mock fileutil cache so we can through failures
        Cache spyCache = Mockito.spy(new Cache());
        FileUtil.cache = spyCache;
        doThrow(DavaException.class).when(spyCache).invalidate(table.getTablePath(partition));


        // get all rows
        List<Row> allRowBefore = new All().retrieve(table, List.of(), null, null);


        // do insert
        int INSERT_ITERATIONS = 100;
        List<Row> rows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);
        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert = new Insert(database, table, partition);
        try {
            insert.insert(rows);
        } catch (DavaException e) {
            log.trace("caught");
        }


        FileUtil.cache = new Cache();

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

        // assert none all the rows before are in the table after
        allRowBefore.forEach(beforeRow ->
            assertTrue(
                allRowAfter.stream()
                    .anyMatch(afterRow -> afterRow.equals(beforeRow))
            )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    void rollback_insert_failure_during_batchWriteToIndices(Mode tableMode) throws IOException {
        setUp(tableMode);

        // mock fileutil cache so we can through failures
        Cache spyCache = Mockito.spy(new Cache());
        FileUtil.cache = spyCache;
        doCallRealMethod()
            .doCallRealMethod()
            .doCallRealMethod()
            .doThrow(DavaException.class).when(spyCache).invalidate(any());


        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);


        // get all rows
        List<Row> allRowBefore = new All().retrieve(table, List.of(), null, null);


        // do insert
        int INSERT_ITERATIONS = 100;
        List<Row> rows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);
        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert = new Insert(database, table, partition);
        try {
            insert.insert(rows);
        } catch (DavaException e) {
            log.trace("caught");
        }


        FileUtil.cache = new Cache();

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

        // assert none all the rows before are in the table after
        allRowBefore.forEach(beforeRow ->
                                 assertTrue(
                                     allRowAfter.stream()
                                         .anyMatch(afterRow -> afterRow.equals(beforeRow))
                                 )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    void insert_delete_insert(Mode tableMode) throws IOException {
        setUp(tableMode);

        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();

        // do insert
        int INSERT_ITERATIONS = 100;
        List<Row> firstInsertRows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);
        firstInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert = new Insert(database, table, partition);
        insert.insert(firstInsertRows);

        // do delete
        Equals equals = new Equals("discount", "1");
        List<Row> rowsToDelete = equals.retrieve(table, new ArrayList<>(), null, null);
        Delete delete = new Delete(database, table);
        delete.delete(rowsToDelete);

        long intialSize = table.getSize(partition);

        // do insert
        List<Row> secondInsertRows = makeRows(seed + ITERATIONS + INSERT_ITERATIONS, INSERT_ITERATIONS);
        firstInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert secondInsert = new Insert(database, table, partition);
        secondInsert.insert(secondInsertRows);

        // assert table is the correct size after rollback
        long size = table.getSize(partition);
        assertEquals(intialSize + INSERT_ITERATIONS, size);

        // assert that each empty row in the table is in the empties file
        if (tableMode != Mode.LIGHT) {
            List<Route> allEmpties = BaseOperationService.getAllEmpties(table.emptiesFilePath(partition));
            assertNotNull(allEmpties);
            byte[] tableBytes = FileUtil.readBytes(table.getTablePath(partition));
            byte whiteSpace = " ".getBytes(StandardCharsets.UTF_8)[0];
            byte newLine = "\n".getBytes(StandardCharsets.UTF_8)[0];
            for(int i = 0; i < tableBytes.length; i++) {
                if (tableBytes[i] == newLine) {
                    i++;
                    if (i >= tableBytes.length)
                        break;

                    if (tableBytes[i] == whiteSpace) {
                        long offset = i;
                        assertTrue(
                            allEmpties.stream()
                                .anyMatch(route -> route.getOffsetInTable() == offset)
                        );
                    }
                }
            }
        }

    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
//    @ValueSource(strings = {"INDEX_ALL"})
    void insert_delete_insert_rollback_insert(Mode tableMode) throws IOException {
        setUp(tableMode);


        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();


        // do insert
        int INSERT_ITERATIONS = 100;
        List<Row> firstInsertRows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);
        firstInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert = new Insert(database, table, partition);
        insert.insert(firstInsertRows);


        // do delete
        Equals equals = new Equals("discount", "1");
        List<Row> rowsToDelete = equals.retrieve(table, new ArrayList<>(), null, null);
        Delete delete = new Delete(database, table);
        delete.delete(rowsToDelete);


        long intialSize = table.getSize(partition);

        // do insert
        List<Row> secondInsertRows = makeRows(seed + ITERATIONS + INSERT_ITERATIONS, INSERT_ITERATIONS);
        firstInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert secondInsert = new Insert(database, table, partition);
        secondInsert.insert(secondInsertRows);


        // rollback
        Timer timer = Timer.start();
        Rollback rollback = new Rollback();
        rollback.rollback(table, partition, table.getRollbackPath(partition));
        timer.printRestart();

        // assert table is the same size after rollback
        long size = table.getSize(partition);
        assertEquals(intialSize, size);

    }


    // rollback halfway through transactions
    // rollback with using empties
    // delete, insert using empties
    //

    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
//    @ValueSource(strings = {"INDEX_ALL"})
    void repetitive_reads(Mode tableMode) throws IOException {
        setUp(tableMode);


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

        // with cache
        timer = Timer.start();
        for (int i = 0; i < 10; i++) {
            List<Row> rows = before.retrieve(table, new ArrayList<>(), null, null);
        }
        timer.printRestart();

        // without using cache
        timer = Timer.start();
        for (int i = 0; i < 10; i++) {
            FileUtil.invalidateCache();
            List<Row> rows = before.retrieve(table, new ArrayList<>(), null, null);
        }
        timer.printRestart();


        assertTrue(true);
    }


    //@Test
    void insert_fails_during_numeric_repartition() {

    }



}
