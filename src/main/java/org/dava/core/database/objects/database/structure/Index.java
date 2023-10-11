package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;

public class Index {



    public static String buildIndexPath(String databaseRoot, String tableName, String partition, String columnName, String value) {
        return buildIndexRootPath(databaseRoot, tableName, partition, columnName) + "/" + value + ".index";
    }

    public static String buildIndexRootPath(String databaseRoot, String tableName, String partition, String columnName) {
        return databaseRoot + "/" + tableName + "/indecis_" + partition + "/" + columnName;
    }

}
