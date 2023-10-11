package org.dava.core.service;

import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Index;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.objects.database.structure.Table;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.MarshallingService;
import org.dava.external.Order;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationServiceTest {

    Database database;


    @BeforeEach
    void setup() {
        database = new Database("db", List.of(Order.class));
    }


    @Test
    void insert() {
        //   181,801 / 293,141 = 62% data, 28% meta
        Random random = new Random();

        String tableName = "";
        for (int i = 0; i < 2000; i++) {
            Order orderTable = new Order(UUID.randomUUID().toString(), BigDecimal.valueOf(Math.abs(random.nextLong())), OffsetDateTime.now());
            Row row = MarshallingService.parseRow(orderTable).get(0);
            tableName = row.getTableName();
            boolean saved = BaseOperationService.insert(row, database, database.getTableByName(tableName), true);
        }

        Table<?> table = database.getTableByName(tableName);
        long size = table.getSize(table.getRandomPartition());

        assertEquals(2000, size);
    }

}
