package org.dava.core.service;

import org.dava.common.StreamUtil;
import org.dava.common.logger.Level;
import org.dava.common.logger.Logger;
import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.IndexRoute;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.objects.database.structure.Table;
import org.dava.core.database.objects.dates.OffsetDate;
import org.dava.core.database.service.BaseOperationService;
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
    static String DB_ROOT = "db";
    private static final Logger log = Logger.getLogger(OperationServiceTest.class.getName());


    static void setUpWipeAndPopulate(Long seed) throws IOException {
        if (FileUtil.exists(DB_ROOT + "/Order")) {
            FileUtil.deleteDirectory(DB_ROOT + "/Order");
        }

        seed = (seed == null)? new Random().nextLong() : seed;

        setUpUseExisting(seed);

        // make 1000 entries
        Random random = new Random(seed);

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
            Insert.insert(List.of(row), database, database.getTableByName(tableName), true);
        }

        log.info("Seed: " + seed);

    }

    static void setUpUseExisting(long seed) {
        database = new Database(DB_ROOT, List.of(Order.class), seed);
    }

    @BeforeEach
    void setUp() throws IOException {
        Logger.setApplicationLogLevel(logLevel);
        setUpWipeAndPopulate(-183502108378805369L);
//        setUpUseExisting(0L);
    }


    @Test
    void insert() throws IOException {
        //   181,801 / 293,141 = 62% data, 28% meta
        //   193,031 / 254,167 = 76% data, 24% meta (with large text column)

        // in attractions the asset table had about 4-14 ms per insert
        // batch inserts are 14ms and don't increase much with a batch size <100
        // so for we're close but a little faster. 1ms for insert! though rollback logs may cost more time

        int INSERT_ITERATIONS = 1000;

        Random random = new Random();

        String tableName = "";
        List<Row> rows = IntStream.range(0, INSERT_ITERATIONS)
            .mapToObj( i -> {
                Order orderTable = new Order(
                    UUID.randomUUID().toString(),
                    "This is a long description. A lot of tables could have something like this so this will be a good test to include this.",
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 100))),
                    BigDecimal.valueOf(Math.abs(random.nextLong(0, 5))),
                    OffsetDateTime.now());
                if (i > 10000 && i % 10000 == 0) {
                    log.print(i + "/" + INSERT_ITERATIONS);
                }
                return MarshallingService.parseRow(orderTable).get(0);
            })
            .toList();

        tableName = rows.get(0).getTableName();
        Timer timer = Timer.start();
        Insert.insert(rows, database, database.getTableByName(tableName), true);
        timer.printRestart();

        Table<?> table = database.getTableByName(tableName);
        long size = table.getSize(table.getRandomPartition());

        assertEquals(INSERT_ITERATIONS + ITERATIONS, size - 1);
    }

    @Test
    void getIndices() {
        Table<?> table = database.getTableByName("Order");
        String partition = table.getRandomPartition();

        String indexPath = "db/Order/indecis_Order/total/0.index";
        List<IndexRoute> indices = BaseOperationService.getRoutes(indexPath, partition, 8L, null);

        indices.forEach( route -> {
            log.debug(route.toString());

            String row = BaseOperationService
                .getLinesFromPartition(partition, table, List.of(route) )
                .collect(Collectors.joining("\n"));
            log.debug(row);
        });

        log.debug("Total Rows: " + indices.size());
    }

    @Test
    void equals() throws IOException {

        Timer timer = Timer.start();
        Equals equals = new Equals("total", "25");
        List<Row> rows = equals.retrieve(database, new ArrayList<>(), "Order", 10L, null);
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
    void and() throws IOException {

        Timer timer = Timer.start();
        And and = new And(
            new Equals("total", "25"),
            new Equals("discount", "1")
        );
        List<Row> rows = and.retrieve(database, new ArrayList<>(), "Order", 10L, 1L);
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
    void after() throws IOException {

        long offset = 5L;
        OffsetDate date = OffsetDate.of(OffsetDateTime.now().minusYears(1).toString());

        Timer timer = Timer.start();
        After<OffsetDate> after = new After<>(
                "time",
                date,
                true
        );
        List<Row> rows = after.retrieve(database, new ArrayList<>(), "Order", 10L, offset);
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
    void before() throws IOException {

        long offset = 5L;
        OffsetDate date = OffsetDate.of(OffsetDateTime.now().minusYears(10).toString());

        Timer timer = Timer.start();
        Before<OffsetDate> before = new Before<>(
                "time",
                date,
                true
        );
        List<Row> rows = before.retrieve(database, new ArrayList<>(), "Order", 10L, offset);
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
