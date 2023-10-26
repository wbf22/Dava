package org.dava.core.database.objects.database.structure;


import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;
import static org.dava.core.database.objects.exception.ExceptionType.TABLE_PARSE_ERROR;


public class Table<T> {

    private LinkedHashMap<String, Column<?>> columns;
    /**
     * Row lengths are added whenever a row is too long for the current length.
     * After a new row length is added, the table will wait 10 more rows before
     * determining a new row length based on the average length of the last ten.
     *
     * The row of the RowLength is the first row of that length
     */

    private Map<String, List<RowLength>> rowLengths;

    /**
     * Used in calculating offset
     */
    private String tableName;
    private String directory;
    private List<String> partitions;
    private Random random;


    public Table(Class<T> tableClass, String databaseRoot) {

        org.dava.external.annotations.Table annotation = Optional.ofNullable(
            tableClass.getAnnotation( org.dava.external.annotations.Table.class )
        ).orElseThrow(
            () -> makeTableParseError("Table class missing @Table annotation: " + tableClass.getName())
        );

        this.tableName = (annotation.name().isEmpty())? tableClass.getSimpleName() : annotation.name();

        // TODO later get this stuff from the master sql file
        columns = new LinkedHashMap<>();
        for (Field field : tableClass.getDeclaredFields()) {
            org.dava.external.annotations.Column columnAnn = field.getAnnotation( org.dava.external.annotations.Column.class );
            String name = (columnAnn == null)? field.getName() : columnAnn.name();
            columns.put(
                name,
                new Column<>(name, field.getType())
            );
        }


        this.directory = databaseRoot + "/" + tableName;
        try {
            FileUtil.createDirectoriesIfNotExist(this.directory);
            makeEmtpiesFileIfDoesntExist(tableName);
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error creating folder for table or empties file: " + tableName,
                e
            );
        }

        // TODO get this from the folder structure, also make settings for this
        partitions = new ArrayList<>();
        partitions.add(tableName);

        initTableCsv(partitions);

        random = new Random(System.currentTimeMillis());

    }

    private void initTableCsv(List<String> partitions) {
        Map<String, List<RowLength>> lengths = new HashMap<>();
        for (String partition : partitions) {
            String path = getTablePath(partition) + ".csv";

            if (!FileUtil.exists(path)) {

                // write column titles
                String columnTitles = columns.values().stream()
                    .map(Column::getName)
                    .reduce("", (acc, n) -> acc + "," + n )
                    .substring(1) + "\n";
                byte[] bytes = columnTitles.getBytes(StandardCharsets.UTF_8);
                try {
                    FileUtil.writeBytes(
                        path,
                        0,
                        bytes
                    );
                } catch (IOException e) {
                    throw new DavaException(
                        BASE_IO_ERROR,
                        "Error writing titles in table: " + tableName,
                        e
                    );
                }

                // set table row count to 1
                setSize(partition, 1L);
            }
        }
        rowLengths = lengths;
    }

    public DavaException makeTableParseError(String message) {
        return new DavaException(
            TABLE_PARSE_ERROR,
            message,
            null
        );
    }

    public String getTablePath(String partitionName) {
        return directory + "/" + partitionName;
    }

    /**
     * Get an empty row in the table.
     * @return next empty row in the table, or null there are no empty rows.
     */
    public IndexRoute getEmptyRow(String partition) {
        return BaseOperationService.popEmpty(getTablePath(partition) + ".empties", 8, random);
    }

    public void makeEmtpiesFileIfDoesntExist(String partition) throws IOException {
        String path = getTablePath(partition) + ".empties";
        if ( !FileUtil.exists( path ) ) {
            FileUtil.writeBytes(
                path,
                0,
                TypeToByteUtil.longToByteArray(0L)
            );
        }
    }

    public void writeEmptyRow(long row) {
        String partition = getRandomPartition();
        BaseOperationService.writeLong(getTablePath(partition), row);
    }

    /**
     * The size of the table is stored in the first 8 bytes of the empties file
     */
    public long getSize(String partition) {
        try {
            byte[] bytes = FileUtil.readBytes(getTablePath(partition) + ".empties", 0, 8);
            return TypeToByteUtil.byteArrayToLong(bytes);
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error getting size for table: " + tableName + partition,
                e
            );
        }
    }

    public void setSize(String partition, Long newSize) {
        try {
            FileUtil.writeBytes(
                getTablePath(partition) + ".empties",
                0,
                TypeToByteUtil.longToByteArray(newSize)
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error getting size for table: " + tableName + partition,
                e
            );
        }
    }

    public String getRandomPartition() {
        return partitions.get(random.nextInt(0, partitions.size()));
    }

    public void saveRowLengths(List<RowLength> lengths, String partition) {
        try {
            FileUtil.writeBytes(
                getTablePath(partition) + ".rowLengths",
                0,
                RowLength.serializeList(lengths)
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error saving row lengths for table : " + tableName + partition,
                e
            );
        }
    }


    public Column<?> getColumn(String columnName) {
        return columns.get(columnName);
    }



    /*
    Getter Setters
     */
    public LinkedHashMap<String, Column<?>> getColumns() {
        return columns;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPartitions() {
        return partitions;
    }


}
