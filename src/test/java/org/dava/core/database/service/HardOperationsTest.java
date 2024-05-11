package org.dava.core.database.service;

import org.dava.api.DavaId;
import org.dava.api.Order;
import org.dava.core.common.Timer;
import org.dava.core.common.logger.Level;
import org.dava.core.common.logger.Logger;
import org.dava.core.database.objects.dates.OffsetDate;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.objects.exception.ExceptionType;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.operations.Delete;
import org.dava.core.database.service.operations.Insert;
import org.dava.core.database.service.operations.Update;
import org.dava.core.database.service.operations.common.Batch;
import org.dava.core.database.service.operations.common.WritePackage;
import org.dava.core.database.service.structure.*;
import org.dava.core.sql.conditions.After;
import org.dava.core.sql.conditions.All;
import org.dava.core.sql.conditions.Before;
import org.dava.core.sql.conditions.Equals;
import org.dava.core.sql.operators.And;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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

    static FileUtil fileUtil = new FileUtil();


    static void setUpWipeAndPopulate(Mode tableMode) throws IOException {
        if (fileUtil.exists(DB_ROOT + "/Order")) {
            fileUtil.deleteDirectory(DB_ROOT + "/Order");
        }

        seed = (seed == null)? new Random().nextLong() : seed;

        setUpUseExisting(tableMode);

        Table<?> table = database.getTableByName("Order");
        List<Row> rows = makeRows(seed, ITERATIONS);

        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });

        Insert insert = new Insert(database, table, table.getRandomPartition());
        insert.addToBatch(rows, true, new Batch()).execute(table, true);

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
                    DavaId.generateId("order", String.valueOf(random.nextInt(1000000000))),
                    "This is a long description. A lot of tables could have something like this, so this will be a good test.",
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 100))),
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 5))),
                    time
                );
                return MarshallingService.parseRow(orderTable).get("Order").get(0);
            })
            .toList();
    }

    void setUp(Mode tableMode) throws IOException {

        Logger.setApplicationLogLevel(logLevel);
        fileUtil = new FileUtil();
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


    /*
     * Tests doing a delete and then some normal operations afterwards
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    // @ValueSource(strings = {"LIGHT"})
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

        Delete delete = new Delete(database, table);
        delete.addToBatch(rows, true, new Batch()).execute(table, true);


        Equals equals = new Equals("discount", "4");
        List<Row> postDeleteRows = equals.retrieve(table, new ArrayList<>(), null, null);
        postDeleteRows.forEach( row -> {
            OffsetDate rowDate = OffsetDate.of(row.getValue("time").toString());
            assertTrue(rowDate.isAfter(date));
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

    /*
     * Tests rollback with an insert fails when trying to add rows to a table
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    void rollback_insert_failure_during_addToTable(Mode tableMode) throws IOException {

        setUp(tableMode);

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

        // mock fileutil so we can through failures
        FileUtil spyFileUtil = Mockito.spy(new FileUtil());
        insert.fileUtil = spyFileUtil;
        doThrow(DavaException.class).when(spyFileUtil).writeBytes(
                eq(table.getTablePath(partition)), 
                anyList()
            );

        try {
            insert.addToBatch(rows, true, new Batch()).execute(table, true);;
        } catch (DavaException e) {
            log.trace("caught");
        }

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

        // assert all the rows before are in the table after
        allRowBefore.forEach(beforeRow ->
            assertTrue(
                allRowAfter.stream()
                    .anyMatch(afterRow -> afterRow.equals(beforeRow))
            )
        );
    }

    /*
     * Tests rollback with an inserts fails when trying to write to index files
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    void rollback_insert_failure_during_batchWriteToIndices(Mode tableMode) throws IOException {
        setUp(tableMode);

      

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


        // mock fileutil so we can through failures
        FileUtil spyFileUtil = Mockito.spy(new FileUtil());
        BaseOperationService.fileUtil = spyFileUtil;
        doThrow(DavaException.class).when(spyFileUtil).writeBytes(
            any(), 
            anyList()
        );


        try {
            insert.addToBatch(rows, true, new Batch()).execute(table, true);;
        } catch (DavaException e) {
            log.trace("caught");
        }

        BaseOperationService.fileUtil = new FileUtil();


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

        // assert all the rows before are in the table after
        allRowBefore.forEach(beforeRow ->
            assertTrue(
                allRowAfter.stream()
                    .anyMatch(afterRow -> afterRow.equals(beforeRow))
            )
        );
    }

    /*
     * Tests doing an insert delete insert combo ensuring the correct rows are present afterwards
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    // @ValueSource(strings = {"LIGHT"})
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
        insert.addToBatch(firstInsertRows, true, new Batch()).execute(table, true);;

        // do delete
        Equals equals = new Equals("discount", "1");
        List<Row> rowsToDelete = equals.retrieve(table, new ArrayList<>(), null, null);
        Delete delete = new Delete(database, table);
        delete.addToBatch(rowsToDelete, true, new Batch()).execute(table, true);;

        long intialSize = table.getSize(partition);

        // do insert
        List<Row> secondInsertRows = makeRows(seed + ITERATIONS + INSERT_ITERATIONS, INSERT_ITERATIONS);
        firstInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert secondInsert = new Insert(database, table, partition);
        secondInsert.addToBatch(secondInsertRows, true, new Batch()).execute(table, true);;

        // assert table is the correct size after second insert
        long size = table.getSize(partition);
        assertEquals(intialSize + INSERT_ITERATIONS, size);

        // assert that each empty row in the table is in the empties file
        if (tableMode != Mode.LIGHT) {
            List<Route> allEmpties = BaseOperationService.getAllEmpties(table.emptiesFilePath(partition));
            assertNotNull(allEmpties);
            byte[] tableBytes = fileUtil.readBytes(table.getTablePath(partition));
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

    /*
     * Tests doing and insert and delete, and then doing a second insert which is rolled back
     */
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
        insert.addToBatch(firstInsertRows, true, new Batch()).execute(table, true);


        // do delete
        Equals equals = new Equals("discount", "1");
        List<Row> rowsToDelete = equals.retrieve(table, new ArrayList<>(), null, null);
        Delete delete = new Delete(database, table);
        delete.addToBatch(rowsToDelete, true, new Batch()).execute(table, true);


        long intialSize = table.getSize(partition);

        // do insert
        List<Row> secondInsertRows = makeRows(seed + ITERATIONS + INSERT_ITERATIONS, INSERT_ITERATIONS);
        firstInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert secondInsert = new Insert(database, table, partition);
        secondInsert.addToBatch(secondInsertRows, true, new Batch()).execute(table, true);


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

    /*
     * Speed test, to show the benefit of the cache
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
//    @ValueSource(strings = {"INDEX_ALL"})
    void repetitive_reads(Mode tableMode) throws IOException {
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
        insert.addToBatch(firstInsertRows, true, new Batch()).execute(table, true);;



        // warm up
        OffsetDate date = OffsetDate.of(OffsetDateTime.now().minusYears(10).toString());
        Before<OffsetDate> before = new Before<>(
            "time",
            date,
            true
        );
        for (int i = 0; i < 10; i++) {
            List<Row> rows = before.retrieve(table, new ArrayList<>(), null, null);
        }

        Timer timer;
        int TEST_ITERATIONS = 100;

        // without using cache
        log.info("Without Cache: ");
        timer = Timer.start();
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            List<Row> rows = before.retrieve(table, new ArrayList<>(), null, null);
        }
        timer.printRestart();


        assertTrue(true);
    }



    /*
     * Test doing insert and delete in a single transaction and rolling everything back
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
//    @ValueSource(strings = {"LIGHT"})
    void insert_delete_in_transaction_then_rollback(Mode tableMode) throws IOException {
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
        insert.addToBatch(firstInsertRows, true, new Batch()).execute(table, true);

        long intialSize = table.getSize(partition);


        // do insert start of transaction
        List<Row> transactionInsertRows = makeRows(seed + ITERATIONS+ INSERT_ITERATIONS, INSERT_ITERATIONS);
        transactionInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        insert = new Insert(database, table, partition);
        insert.addToBatch(transactionInsertRows, true, new Batch()).execute(table, true);


        // do delete
        Equals equals = new Equals("discount", "1");
        List<Row> rowsToDelete = equals.retrieve(table, new ArrayList<>(), null, null);
        Delete delete = new Delete(database, table);
        delete.addToBatch(rowsToDelete, false, new Batch()).execute(table, false);

        // rollback transaction (2nd insert and delete operation)
        Timer timer = Timer.start();
        Rollback rollback = new Rollback();
        rollback.rollback(table, partition, table.getRollbackPath(partition));
        timer.printRestart();

        // assert table is the same size after rollback
        long size = table.getSize(partition);
        assertEquals(intialSize, size);
    }


    /*
     * Test rolling back insert that has a failure during a numeric repartition
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    void insert_fails_during_numeric_repartition(Mode tableMode) throws IOException {

        setUp(tableMode);

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


        // mock fileutil so we can through failures
        FileUtil spyFileUtil = Mockito.spy(new FileUtil());
        BaseOperationService.fileUtil = spyFileUtil;
        doThrow(DavaException.class).when(spyFileUtil).deleteFile(
            anyString()
        );

        try {
            insert.addToBatch(rows, true, new Batch()).execute(table, true);;
        } catch (DavaException e) {
            log.trace("caught");
        }

        BaseOperationService.fileUtil = new FileUtil();

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

        // assert all the rows before are in the table after
        allRowBefore.forEach(beforeRow ->
            assertTrue(
                allRowAfter.stream()
                    .anyMatch(afterRow -> afterRow.equals(beforeRow))
            )
        );
    }


    /*
     * Test with 2 inserts in a transaction, the second that has a failure during a 
     * numeric repartition.
     * 
     * A rollback is then performed and everything is checked
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    // @ValueSource(strings = {"INDEX_ALL"})
    void second_insert_fails_during_numeric_repartition_(Mode tableMode) throws IOException {

        setUp(tableMode);

        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);

        // get all rows
        List<Row> allRowBefore = new All().retrieve(table, List.of(), null, null);


        // do insert
        int INSERT_ITERATIONS = 100;
        List<Row> firstInsertRows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);
        firstInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert = new Insert(database, table, partition);
        insert.addToBatch(firstInsertRows, true, new Batch()).execute(table, true);;

        // do insert
        List<Row> rows = makeRows(seed + ITERATIONS + ITERATIONS, INSERT_ITERATIONS);
        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert2 = new Insert(database, table, partition);


        // mock fileutil so we can through failures
        FileUtil spyFileUtil = Mockito.spy(new FileUtil());
        insert2.fileUtil = spyFileUtil;
        doCallRealMethod()
            .doThrow(DavaException.class).when(spyFileUtil).copyFilesToDirectory(
                any(),
                any()
            );


        try {
            insert2.addToBatch(rows, false, new Batch()).execute(table, false);
        } catch (DavaException e) {
            log.trace("caught");
        }

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

        // assert all the rows before are in the table after
        allRowBefore.forEach(beforeRow ->
            assertTrue(
                allRowAfter.stream()
                    .anyMatch(afterRow -> afterRow.equals(beforeRow))
            )
        );
    }


    /*
     * Three inserts performed in sequence
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    // @ValueSource(strings = {"INDEX_ALL"})
    void triple_insert(Mode tableMode) throws IOException {

        setUp(tableMode);

        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);

        // do insert
        int INSERT_ITERATIONS = 100;
        List<Row> firstInsertRows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);
        firstInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert = new Insert(database, table, partition);
        insert.addToBatch(firstInsertRows, true, new Batch()).execute(table, true);

        List<Row> secondInsertRows = makeRows(seed + ITERATIONS + ITERATIONS, INSERT_ITERATIONS);
        secondInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert2 = new Insert(database, table, partition);
        insert2.addToBatch(secondInsertRows, false, new Batch()).execute(table, false);


        // get all rows
        List<Row> allRowAfter = new All().retrieve(table, List.of(), null, null);
        assertEquals(intialSize + INSERT_ITERATIONS * 2 - 1, allRowAfter.size());

        // assert table size is correct
        long size = table.getSize(partition);
        assertEquals(intialSize + INSERT_ITERATIONS * 2, size);

    }


    /*
     * Two inserts in a transaction. The first succeeds, the seconds fails part way
     * through logging it rollback.
     * 
     * This is then rolled back, with the first insert being rolled back and the second
     * being basically ignored since no action was taken
     */
    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    // @ValueSource(strings = {"INDEX_ALL"})
    void rollback_second_insert_fails_during_log_rollback(Mode tableMode) throws IOException {

        setUp(tableMode);

        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);


        // get all rows
        List<Row> allRowBefore = new All().retrieve(table, List.of(), null, null);


        // do insert
        int INSERT_ITERATIONS = 100;
        List<Row> firstInsertRows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);
        firstInsertRows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert = new Insert(database, table, partition);
        insert.addToBatch(firstInsertRows, true, new Batch()).execute(table, true);

        // do insert
        List<Row> rows = makeRows(seed + ITERATIONS + ITERATIONS, INSERT_ITERATIONS);
        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert2 = new Insert(database, table, partition);


        // mock fileutil so we can through failures
        FileUtil spyFileUtil = Mockito.spy(new FileUtil());
        insert2.fileUtil = spyFileUtil;
        doThrow(DavaException.class).when(spyFileUtil).writeBytes(
            any(),
            any()
        );


        try {
            insert2.addToBatch(rows, false, new Batch()).execute(table, false);
        } catch (DavaException e) {
            log.trace("caught");
        }

        // erase a few lines off the logback file (simulates crash mid way through rollback log)
        long fileSize = fileUtil.fileSize(table.getRollbackPath(partition));
        fileUtil.truncate(table.getRollbackPath(partition), fileSize - 100);


        // rollback
        Rollback rollback = new Rollback();
        rollback.rollback(table, partition, table.getRollbackPath(partition));

        // get all rows
        List<Row> allRowAfter = new All().retrieve(table, List.of(), null, null);

        // assert table is the same size after rollback
        long size = table.getSize(partition);
        assertEquals(intialSize, size);

        // assert all the rows before are in the table after
        allRowBefore.forEach(beforeRow ->
            assertTrue(
                allRowAfter.stream()
                    .anyMatch(afterRow -> afterRow.equals(beforeRow))
            )
        );
    }


    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    // @ValueSource(strings = {"LIGHT"})
    void rollback_after_crash_halfway_through_table_line_write(Mode tableMode) throws IOException {

        setUp(tableMode);

        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);

        // get all rows
        List<Row> allRowBefore = new All().retrieve(table, List.of(), null, null);


        // do insert with failure
        int INSERT_ITERATIONS = 100;
        List<Row> rows = makeRows(seed + ITERATIONS, INSERT_ITERATIONS);
        rows.forEach(row -> {
            log.trace(Row.serialize(table, row.getColumnsToValues()));
        });
        Insert insert = new Insert(database, table, partition);

        // mock fileutil so we can through failures
        FileUtil spyFileUtil = Mockito.spy(new FileUtil());
        insert.fileUtil = spyFileUtil;
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] object = invocation.getArguments();
                String filePath = (String) object[0];
                List<WritePackage> writePackages = (List<WritePackage>) object[1];

                try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
                    writePackages.forEach( writePackage -> {
                            try {
                                // Move to the desired position in the file
                                long offset = (writePackage.getOffsetInTable() == null)? file.length() : writePackage.getOffsetInTable();
                                file.seek(offset);

                                // simulate a crash halfway through writing a line
                                if (new Random(seed).nextBoolean()){
                                    byte[] bytes = writePackage.getData();
                                    bytes = Arrays.copyOf(bytes, bytes.length / 2);

                                    file.write( bytes );
                                    throw new DavaException(ExceptionType.BASE_IO_ERROR, "test", null);
                                }
        
                                // Write data at the current position
                                file.write( writePackage.getData() );
        
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                }

                return null;
            }
        }).when(spyFileUtil).writeBytes(
                eq(table.getTablePath(partition)), 
                anyList()
            );

        try {
            insert.addToBatch(rows, true, new Batch()).execute(table, true);
        } catch (DavaException e) {
            log.trace("caught");
        }

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

        // assert all the rows before are in the table after
        allRowBefore.forEach(beforeRow ->
            assertTrue(
                allRowAfter.stream()
                    .anyMatch(afterRow -> afterRow.equals(beforeRow))
            )
        );
    }



    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    // @ValueSource(strings = {"LIGHT"})
    void rollback_after_crash_halfway_through_delete(Mode tableMode) throws IOException {

        setUp(tableMode);

        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);

        // get all rows
        List<Row> allRowBefore = new All().retrieve(table, List.of(), null, null);


        // mock fileutil so we can through failures
        FileUtil spyFileUtil = Mockito.spy(new FileUtil());
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] object = invocation.getArguments();
                String filePath = (String) object[0];
                List<WritePackage> writePackages = (List<WritePackage>) object[1];

                try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
                    writePackages.forEach( writePackage -> {
                            try {
                                // Move to the desired position in the file
                                long offset = (writePackage.getOffsetInTable() == null)? file.length() : writePackage.getOffsetInTable();
                                file.seek(offset);

                                // simulate a crash halfway through writing a line
                                if (new Random(seed).nextBoolean()){
                                    byte[] bytes = writePackage.getData();
                                    bytes = Arrays.copyOf(bytes, bytes.length / 2);

                                    file.write( bytes );
                                    throw new DavaException(ExceptionType.BASE_IO_ERROR, "test", null);
                                }
        
                                // Write data at the current position
                                file.write( writePackage.getData() );
        
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                }

                return null;
            }
        }).when(spyFileUtil).writeBytes(
                eq(table.getTablePath(partition)), 
                anyList()
            );

        try {
            Equals equals = new Equals("discount", "1");
            List<Row> rowsToDelete = equals.retrieve(table, new ArrayList<>(), null, null);
            Delete delete = new Delete(database, table);
            delete.fileUtil = spyFileUtil;

            delete.addToBatch(rowsToDelete, true, new Batch()).execute(table, true);
        } catch (DavaException e) {
            log.trace("caught");
        }

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

        // assert all the rows before are in the table after
        allRowBefore.forEach(beforeRow ->
            assertTrue(
                allRowAfter.stream()
                    .anyMatch(afterRow -> afterRow.equals(beforeRow))
            )
        );
    }



    @ParameterizedTest
    @ValueSource(strings = {"INDEX_ALL", "MANUAL", "LIGHT"})
    // @ValueSource(strings = {"INDEX_ALL"})
    void rollback_update(Mode tableMode) throws IOException {

        setUp(tableMode);


        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();
        long intialSize = table.getSize(partition);

        Equals equals = new Equals("total", "25");
        List<Row> rows = equals.retrieve(table, new ArrayList<>(), null, null);
        List<Row> newRows = rows.stream()
            .map(row -> {
                Row newRow = row.copy();
                newRow.getColumnsToValues().put("total", BigDecimal.valueOf(150));
                return newRow;
            })
            .toList();

        Update update = new Update(database, table);
        Timer timer = Timer.start();
        update.addToBatch(rows, newRows, true, new Batch()).execute(table, true);
        timer.printRestart();

        List<Row> afterRows = equals.retrieve(table, new ArrayList<>(), null, null);
        assertEquals(0, afterRows.size());

        Equals equals2 = new Equals("total", "150");
        List<Row> afterRows2 = equals2.retrieve(table, new ArrayList<>(), null, null);
        assertEquals(rows.size(), afterRows2.size());

        // rollback
        timer = Timer.start();
        Rollback rollback = new Rollback();
        rollback.rollback(table, partition, table.getRollbackPath(partition));
        timer.printRestart();

        // assert table is the same size after rollback
        long size = table.getSize(partition);
        assertEquals(intialSize, size);

        // assert the updates to rows were reverted
        List<Row> allRowAfter = equals.retrieve(table, new ArrayList<>(), null, null);
        assertEquals(rows.size(), allRowAfter.size());
    }

}
