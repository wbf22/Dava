package org.dava.core.database.objects.database.structure;


import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.objects.EmptiesPackage;
import org.dava.core.database.service.objects.Empty;
import org.dava.core.database.service.type.compression.TypeToByteUtil;
import org.dava.external.annotations.PrimaryKey;
import org.dava.external.annotations.constraints.Unique;

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



    public Table(Class<T> tableClass, String databaseRoot, long seed) {

        org.dava.external.annotations.Table annotation = Optional.ofNullable(
            tableClass.getAnnotation( org.dava.external.annotations.Table.class )
        ).orElseThrow(
            () -> makeTableParseError("Table class missing @Table annotation: " + tableClass.getName())
        );

        this.tableName = (annotation.name().isEmpty())? tableClass.getSimpleName() : annotation.name();

        // TODO later get this stuff from the master sql file
        // build table schema
        columns = new LinkedHashMap<>();
        for (Field field : tableClass.getDeclaredFields()) {
            org.dava.external.annotations.Column columnAnn = field.getAnnotation( org.dava.external.annotations.Column.class );
            String name = (columnAnn == null)? field.getName() : columnAnn.name();

            Unique unique = field.getAnnotation( Unique.class );
            PrimaryKey primaryKey = field.getAnnotation(PrimaryKey.class );
            boolean isUnique = unique != null || primaryKey != null;

            columns.put(
                name,
                new Column<>(name, field.getType(), isUnique)
            );
        }

        // make empties file and rollback log
        this.directory = databaseRoot + "/" + tableName;
        try {
//            FileUtil.createDirectoriesIfNotExist(this.directory);
            makeEmptiesFileIfDoesntExist(tableName);
            FileUtil.createFile( getRollbackPath(tableName) );

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

        // set up the column titles
        initTableCsv(partitions);

        random = new Random(seed);

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
                setSize(partition, 0L, 1L);
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
    public EmptiesPackage getEmptyRows(String partition, int emptiesNeeded) {

        String emptiesFile = emptiesFilePath(partition);

        try {
            return BaseOperationService.getEmpties(emptiesNeeded, emptiesFile, random);
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error getting empty row for insert from table meta file: " + emptiesFile,
                e
            );
        }
    }

    public void popEmptiesRoutes(String partition, List<Empty> emptiesPackages) {

        String emptiesFile = emptiesFilePath(partition);
        String rollbackPath = getRollbackPath(partition);

        try {
            BaseOperationService.popRoutes(rollbackPath, emptiesFile, emptiesPackages);
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error getting empty row for insert from table meta file: " + emptiesFile,
                e
            );
        }


    }


    public void makeEmptiesFileIfDoesntExist(String partition) throws IOException {
        String folder = emptiesFolder(partition);
        FileUtil.createDirectoriesIfNotExist(folder);

        String path = folder + "/" + partition + ".empties";
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
        BaseOperationService.writeLong(emptiesFilePath(partition), row);
    }
    private String emptiesFolder(String partition) {
        return directory + "/indices_" + partition;
    }
    private String emptiesFilePath(String partition) {
        return directory + "/indices_" + partition + "/" + partition + ".empties";
    }

    /**
     * The size of the table is stored in the first 8 bytes of the empties file
     */
    public long getSize(String partition) {
        try {
            byte[] bytes = FileUtil.readBytes(emptiesFilePath(partition), 0, 8);
            return TypeToByteUtil.byteArrayToLong(bytes);
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error getting size for table: " + tableName + partition,
                e
            );
        }
    }

    public void setSize(String partition, Long oldSize, Long newSize) {
        try {
            BaseOperationService.performOperation(
                () -> FileUtil.writeBytes(
                    emptiesFilePath(partition),
                    0,
                    TypeToByteUtil.longToByteArray(newSize)
                ),
                "S:" + oldSize + "," + newSize + "\n",
                getRollbackPath(partition)
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

    public String getRollbackPath(String partition) {
        return directory + "/indices_" + partition + "/" + partition + ".rollback";
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

    public Random getRandom() {
        return random;
    }
}
