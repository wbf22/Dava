package org.dava.core.database.objects.database.structure;


import org.dava.common.TypeUtil;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.objects.EmptiesPackage;
import org.dava.core.database.service.type.compression.TypeToByteUtil;
import org.dava.external.annotations.PrimaryKey;
import org.dava.external.annotations.constraints.Unique;
import org.dava.external.annotations.indices.Indexed;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;
import static org.dava.core.database.objects.exception.ExceptionType.TABLE_PARSE_ERROR;


public class Table<T> {

    private LinkedHashMap<String, Column<?>> columns;
    private final String tableName;
    private final String directory;
    private final String databaseRoot;
    private final Mode mode;
    private List<String> partitions;
    private final Random random;
    private Map<String, List<File>> columnLeaves = new HashMap<>(); // for numeric folders



    public Table(Class<T> tableClass, String databaseRoot, Mode mode, long seed) {
        FileUtil.invalidateCache();  // making sure cache hasn't cached anything from file with the same names

        org.dava.external.annotations.Table annotation = Optional.ofNullable(
            tableClass.getAnnotation( org.dava.external.annotations.Table.class )
        ).orElseThrow(
            () -> makeTableParseError("Table class missing @Table annotation: " + tableClass.getName())
        );

        this.tableName = (annotation.name().isEmpty())? tableClass.getSimpleName() : annotation.name();
        this.databaseRoot = databaseRoot;
        this.mode = mode;
        this.directory = databaseRoot + "/" + tableName;

        // TODO get this from the folder structure, also make settings for this
        partitions = new ArrayList<>();
        partitions.add(tableName);

        // TODO later get this stuff from the master sql file
        // build table schema
        columns = new LinkedHashMap<>();
        for (Field field : tableClass.getDeclaredFields()) {
            org.dava.external.annotations.Column columnAnn = field.getAnnotation( org.dava.external.annotations.Column.class );
            String name = (columnAnn == null)? field.getName() : columnAnn.name();

            Unique unique = field.getAnnotation( Unique.class );
            PrimaryKey primaryKey = field.getAnnotation(PrimaryKey.class );
            boolean isUnique = unique != null || primaryKey != null;

            Indexed indexed = field.getAnnotation( Indexed.class );
            boolean isIndexed = mode == Mode.INDEX_ALL || indexed != null || primaryKey != null;
            isIndexed = mode != Mode.LIGHT && isIndexed; // if it's light mode don't index anything

            columns.put(
                name,
                new Column<>(name, field.getType(), isIndexed, isUnique)
            );

            // make numeric index count files
            if ( isIndexed && TypeUtil.isNumericClass(field.getType()) ) {
                partitions.forEach( partition -> {
                    String indexPath = Index.buildColumnPath(databaseRoot, tableName, partition, name);
                    try {
                        List<File> subDirs = FileUtil.getSubFolders(indexPath);
                        columnLeaves.put(partition + name, subDirs);

                        if (subDirs.isEmpty()) {
                            // make count file if doesn't exist
                            FileUtil.createDirectoriesIfNotExist(indexPath);
                            if (!FileUtil.exists(indexPath + "/c.count"))
                                FileUtil.createFile(indexPath + "/c.count", TypeToByteUtil.longToByteArray(0L));
                        }

                    } catch (IOException e) {
                        throw new DavaException(
                            BASE_IO_ERROR,
                            "Error creating count file for table index: " + indexPath,
                            e
                        );
                    }
                });
            }
        }

        // make empties file and rollback log
        try {
            String folder = indicesFolder(tableName);
            FileUtil.createDirectoriesIfNotExist(folder);

            if (mode != Mode.LIGHT)
                makeEmptiesFileIfDoesntExist(tableName);

            FileUtil.createFile( getRollbackPath(tableName) );

        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error creating folder for rollback or empties file: " + tableName,
                e
            );
        }


        // set up the column titles
        partitions.forEach(this::initTableCsv);

        random = new Random(seed);

    }

    public void initTableCsv(String partition) {
        try {
            String path = getTablePath(partition);

            if (!FileUtil.exists(path)) {

                // write column titles
                String columnTitles = columns.values().stream()
                    .map(Column::getName)
                    .reduce("", (acc, n) -> acc + "," + n )
                    .substring(1) + "\n";
                byte[] bytes = columnTitles.getBytes(StandardCharsets.UTF_8);
                FileUtil.writeBytes(
                    path,
                    0,
                    bytes
                );

                // set table row count to 1
                setSize(partition, 1L);
            }
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error writing titles in table: " + tableName,
                e
            );
        }
    }

    public void initColumnLeaves() {

        partitions.forEach( partition -> {
            columns.values().forEach( column -> {
                String indexPath = Index.buildColumnPath(databaseRoot, tableName, partition, column.getName());

                List<File> subDirs = FileUtil.getSubFolders(indexPath);
                columnLeaves.put(partition + column.getName(), subDirs);
            });
        });

    }

    public DavaException makeTableParseError(String message) {
        return new DavaException(
            TABLE_PARSE_ERROR,
            message,
            null
        );
    }

    public String getTablePath(String partitionName) {
        return directory + "/" + partitionName + ".csv";
    }

    /**
     * Get an empty row in the table.
     * @return next empty row in the table, or null there are no empty rows.
     */
    public EmptiesPackage getEmptyRows(String partition) {

        String emptiesFile = emptiesFilePath(partition);

        try {
            List<IndexRoute> routes = BaseOperationService.getAllEmpties(emptiesFile);
            EmptiesPackage emptiesPackage = new EmptiesPackage();
            if (routes == null)
                return emptiesPackage;

            routes.forEach(emptiesPackage::addEmpty);

            return emptiesPackage;
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error getting empty row for insert from table meta file: " + emptiesFile,
                e
            );
        }
    }

    public void makeEmptiesFileIfDoesntExist(String partition) throws IOException {
        String folder = indicesFolder(partition);

        String path = folder + "/" + partition + ".empties";
        if ( !FileUtil.exists( path ) ) {
            FileUtil.writeBytes(
                path,
                0,
                TypeToByteUtil.longToByteArray(0L)
            );
        }
    }

    private String indicesFolder(String partition) {
        return directory + "/META_" + partition;
    }
    public String emptiesFilePath(String partition) {
        return indicesFolder(partition) + "/" + partition + ".empties";
    }

    /**
     * The size of the table is stored in the first 8 bytes of the empties file
     */
    public long getSize(String partition) {
        try {
            if (mode == Mode.LIGHT) {
                String tableFile = FileUtil.readFile(getTablePath(partition));
                return tableFile.split("\n").length;
            }
            else {
                byte[] bytes = FileUtil.readBytes(emptiesFilePath(partition), 0, 8);
                return TypeToByteUtil.byteArrayToLong(bytes);

            }
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error getting size for table: " + tableName + partition,
                e
            );
        }

    }

    public void setSize(String partition, Long newSize) {
        if (mode == Mode.LIGHT)
            return;

        try {
            FileUtil.writeBytes(
                emptiesFilePath(partition),
                0,
                TypeToByteUtil.longToByteArray(newSize)
            );

        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error setting size for table: " + tableName + partition,
                e
            );
        }
    }

    public String getRandomPartition() {
        return partitions.get(random.nextInt(0, partitions.size()));
    }

    public Column<?> getColumn(String columnName) {
        return columns.get(columnName);
    }

    public String getRollbackPath(String partition) {
        return indicesFolder(partition) + "/" + partition + ".rollback";
    }

    public List<File> getLeafList(String partition, String columnName) {
        return columnLeaves.get(partition + columnName);
    }


    public static byte[] getWhitespaceBytes(int length) {
        byte[] whitespaceBytes = new byte[length];
        Arrays.fill(whitespaceBytes, (byte) 32);
        whitespaceBytes[length - 1] = '\n';
        return whitespaceBytes;
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

    public Mode getMode() {
        return mode;
    }

    public String getDatabaseRoot() {
        return databaseRoot;
    }
}
