package org.dava.core.database.objects.database.structure;


import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;
import static org.dava.core.database.objects.exception.ExceptionType.TABLE_PARSE_ERROR;


public class Table<T> {

    private List<Column<?>> columns;
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
        columns = new ArrayList<>();
        for (Field field : tableClass.getDeclaredFields()) {
            org.dava.external.annotations.Column columnAnn = field.getAnnotation( org.dava.external.annotations.Column.class );
            String name = (columnAnn == null)? field.getName() : columnAnn.name();
            columns.add(
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


        // TODO load from storage file
        rowLengths = loadOrInitRowLengths(partitions);

        random = new Random(System.currentTimeMillis());

    }

    private Map<String, List<RowLength>> loadOrInitRowLengths(List<String> partitions) {
        Map<String, List<RowLength>> lengths = new HashMap<>();
        for (String partition : partitions) {
            if (FileUtil.exists( getTablePath(partition) + ".rowLengths" )) {
                lengths.put(
                    partition,
                    parseRowLengths(partition)
                );
            }
            else {
                lengths.put(
                    partition,
                    new ArrayList<>(List.of(new RowLength(-1, 0)))
                );
            }
        }
        return lengths;
    }

    public List<RowLength> parseRowLengths(String partition) {
        try {
            byte[] bytes = FileUtil.readBytes(
                getTablePath(partition)
            );

            return RowLength.deserializeList(bytes);
        } catch (Exception e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error getting row lengths for table : " + tableName + partition,
                e
            );
        }
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
    public Long getEmptyRow(String partition) {
        return BaseOperationService.popLong(getTablePath(partition) + ".empties", 8, random);
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

    public long getRowLength(long row, String partition) {
        List<RowLength> lengths = rowLengths.get(partition);
        for (int i = 0; i < lengths.size(); i++) {
            RowLength rowL = lengths.get(i);
            long nextRow = (i+1 < lengths.size())? lengths.get(i+1).getRow() : getSize(partition);

            if (nextRow > row) {
                return rowL.getLength();
            }
        }
        return lengths.get(lengths.size() - 1).getLength();
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


    public void addRowLengthIfNeeded(int length, Long destinationRow, String partition) {
        List<RowLength> lengths = rowLengths.get(partition);
        RowLength last = lengths.get(lengths.size() - 1);
        if (length > last.getLength()) {
            lengths.add(
                new RowLength(destinationRow, length)
            );
            saveRowLengths(lengths, partition);
        }
        else if (destinationRow - last.getRow() > 10) {
            int newLength = (last.getLength() + length) / 2;
            lengths.add(
                new RowLength(destinationRow, newLength)
            );
            saveRowLengths(lengths, partition);
        }
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





    /*
    Getter Setters
     */

    public List<RowLength> getRowLengths(String partition) {
        return rowLengths.get(partition);
    }
    public List<Column<?>> getColumns() {
        return columns;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPartitions() {
        return partitions;
    }


}
