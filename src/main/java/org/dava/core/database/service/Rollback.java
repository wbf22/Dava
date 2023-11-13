package org.dava.core.database.service;

import org.dava.core.database.objects.database.structure.Table;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.objects.exception.ExceptionType;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.objects.Batch;
import org.dava.core.database.service.objects.delete.DeleteBatch;
import org.dava.core.database.service.objects.insert.InsertBatch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.dava.core.database.objects.exception.ExceptionType.ROLLBACK_ERROR;

public class Rollback {


    public void rollback(Table<?> table, String partition, String rollbackPath) {
        Batch rollbackBatch = parse(rollbackPath);
        rollbackBatch.rollback(table, partition);
    }


    public static Batch parse(String rollbackPath) {

        try {
            List<String> lines = List.of(
                new String(
                    FileUtil.readBytes(rollbackPath), StandardCharsets.UTF_8
                ).split("\n")
            );

            return switch(lines.get(0)) {
                case "Insert Batch:" -> InsertBatch.parse(lines);
                case "Delete Batch:" -> DeleteBatch.parse(lines);
                default -> throw new DavaException(ROLLBACK_ERROR, "Missing header on rollback file", null);
            };
        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error parsing rollback file: " + rollbackPath, e);
        }
    }



}
