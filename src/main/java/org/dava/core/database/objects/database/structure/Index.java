package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;

public class Index {

    private final int rowLengths;


    public Index(int rowLengths) {
        this.rowLengths = rowLengths;
    }

    /**
     * An index header has the following rows:
     *
     * rowLength
     *
     */
    public static Index parseFromHeader(String countFilePath) {
        try {
            String headerString = FileUtil.readLine(countFilePath, 0);
            int indexRowLengths = TypeToByteUtil.byteArrayToInt(headerString.getBytes(StandardCharsets.UTF_8));

            return new Index(indexRowLengths);
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error reading index header in .count file for path: " + countFilePath,
                e
            );
        }

    }


    public static String buildIndexPath(String databaseRoot, String tableName, String partition, String columnName, String value) {
        return buildIndexRootPath(databaseRoot, tableName, partition, columnName, value) + "/" + value + ".csv";
    }

    public static String buildIndexRootPath(String databaseRoot, String tableName, String partition, String columnName, String value) {
        return databaseRoot + "/" + tableName + "/indecis_" + partition + "/" + columnName + "/" + value;
    }


    public int getRowLengths() {
        return rowLengths;
    }
}
