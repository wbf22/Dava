package org.dava.core.database.service;

import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.operations.common.Batch;
import org.dava.core.database.service.structure.Table;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.dava.core.database.objects.exception.ExceptionType.ROLLBACK_ERROR;

public class Rollback {

    public static FileUtil fileUtil = new FileUtil();


    public void rollback(Table<?> table, String partition, String rollbackPath) {
        List<Batch> batches = parse(rollbackPath, table, partition);

        // undo the batches in chronological order
        Collections.reverse(batches);

        batches.stream()
            .forEach(batch -> {
                batch.rollback(table, partition);
            });
    }


    public static List<Batch> parse(String rollbackPath, Table<?> table, String partition) {

        try {
            
            List<String> batches = new ArrayList<>( 
                List.of(
                    new String(
                        fileUtil.readBytes(rollbackPath), StandardCharsets.UTF_8
                    ).split("--")
                )
            );

            // remove the last empty value from the split
            // (this also ensure that failures during logging the rollback string are not attempted to rollback)
            batches.remove(batches.size()-1);

            return batches.stream()
                .map( batchString -> {
                    List<String> lines = List.of(batchString.split("\n"));
                    return Batch.parse(lines, table, partition);
                })
                .collect(Collectors.toList());

        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error parsing rollback file: " + rollbackPath, e);
        }
    }


    public static void handleNumericRepartitionFailure(Table<?> table, String partition) {
        // on rollback after a crash, if not all were moved, delete all in new partitions, else delete all in current partition
        // NOTE: this goes through every repartition in the rollback file, so if multiple repartitions occured in a transaction,
        // then they'll all be checked again
        List<String> lines = new ArrayList<>();
        try {
            lines = List.of(
                new String(
                    fileUtil.readBytes(table.getNumericRollbackPath(partition)), StandardCharsets.UTF_8
                ).split("\n")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            for (String line : lines) {
                String folderPath = line.split(":")[1];
                File[] files = fileUtil.listFiles(folderPath);
                List<File> filesInNewPartitions = new ArrayList<>();
                List<File> filesInCurrent = new ArrayList<>();
                List<File> directories = new ArrayList<>();
                for (File file : files) {
                    if (!file.isDirectory()) {
                        filesInCurrent.add(file);
                    }
                    else {
                        directories.add(file);
                        filesInNewPartitions.addAll(
                            List.of(fileUtil.listFiles(file.getPath()))
                        );
                    }
                }

                // if all were moved, delete files in current partition
                // filesInNewPartitions.size() should be filesInCurrent.size() + 1 if moved (since we have another count file now)
                // if all were moved and some were deleted from current, then we can safely delete all in current

                if (directories.size() == 2 && filesInCurrent.isEmpty()) {
                    // don't do anything since this repartition was successful
                }
                else if (filesInNewPartitions.size() >= filesInCurrent.size() + 1) {
                    for (File file : filesInCurrent) {
                        fileUtil.deleteFile(file);
                    }
                }
                else {
                    for (File file : filesInNewPartitions) {
                        fileUtil.deleteFile(file);
                    }
                    for (File file : directories) {
                        fileUtil.deleteFile(file);
                    }
                }
            }
        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error in rolling back repartition that failed during previous operation", e);
        }

    }

}
