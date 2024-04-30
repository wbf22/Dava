package org.dava.random.systemtests;

import org.dava.core.common.Timer;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.StreamSupport;

class IOSpeedTests {
    /**
     * Takeaways:
     *  - RandomAccess reading is the fastest (get's complicated with deletes and inserts,
     *    use whitespace or store offsets in indices)
     *  - separating rows into their own files can be really slow like x500 slower
     *  - reading all lines is like x30(for 50k rows)-inf times slower than reading random
     *    access, depending on file size
     *  - RandomAccess is the same speed for a large file and a small one (partitions don't help
     *    for speed if you use RandomAccess. Partitions only handy for distribution across
     *    servers)
     *  - Reading all bytes and then splitting by newlines is comparable in speed to reading a single
     *    line from a file until you get like 166 lines.
     *  - making an empty file is faster than an empty directory
     *  - reading a file name is slightly faster than reading a one line file
     *  - reading individual index files is can be x500 times slower than reading one large index
     *  - reading a specific line (random access) is slower than reading all lines until the file is over 1000 lines
     *  - writing to n separate files is like 300x slower than writing n lines to one file
     *  - jumping (seek) within a file is just as costly as opening new files to write
     *  - writing an object to file with java serialization can be pretty slow, 16ms. Reading is 3ms. ToString and writing
     *    as bytes is 7ms
     *  - reading count file is much faster than list sub files
     *  - reading a file's size is a lot faster than reading the first 8 bytes
     */


    @Test
    void read_large() throws IOException {
        /*
            Test for 50,000 files reading all lines:
                x files: 2246
                Large file read all: 4
                Large file lines one at a time BufferedReader: 17
         */


        int NUM_ROWS = 50000;

        // make 5000 test files
        String row = "$450,02/18/2020";
        for (int i = 0; i < NUM_ROWS; i++) {
            FileUtil.writeFile(i + ".csv", i + "," + row);
        }

        // test reading from 5000 files
        long time = System.currentTimeMillis();
        for (int i = 0; i < NUM_ROWS; i++) {
            String rRow = FileUtil.readFile(i + ".csv");
        }
        System.out.print("x files: ");
        System.out.println(System.currentTimeMillis() - time);


        // make file with 5000 lines
        String rows = "";
        for (int i = 0; i < NUM_ROWS; i++) {
            rows += i + "," + row + "\n";
        }
        FileUtil.writeFile(NUM_ROWS + ".csv", rows);

        // test reading 5000 lines from a file
        time = System.currentTimeMillis();
        String content = FileUtil.readFile(NUM_ROWS + ".csv");
        String[] lines = content.split("\n");
        for (String line : lines) {
            String rRow = line;
        }
        System.out.print("Large file read all: ");
        System.out.println(System.currentTimeMillis() - time);


        time = System.currentTimeMillis();
        File file = new File(NUM_ROWS + ".csv");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String rRow = reader.readLine();
        while (rRow != null) {
            rRow = reader.readLine();
        }
        System.out.print("Large file lines one at a time: ");
        System.out.println(System.currentTimeMillis() - time);

        for (int i = 0; i < NUM_ROWS; i++) {
            FileUtil.deleteFile(i + ".csv");
        }

    }

    @Test
    void read_one_line_vs_all() throws IOException {
        /*
            Test for 50,000 rows, 1000 iterations, trying to get single line:
                Large file read all then find line 2500: 320
                Large file read line 2500: 11
         */

        int NUM_ROWS = 50000;
        int ITERATIONS = 1000;

        // make file with NUM_ROWS lines
        String rows = "";
        String row = "$450,02/18/2020";
        for (int i = 0; i < NUM_ROWS; i++) {
            rows += i + "," + row + "\n";
        }
        FileUtil.writeFile(NUM_ROWS + ".csv", rows);

        // test reading up to line NUM_ROWS/2
        long time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            String content = FileUtil.readFile(NUM_ROWS + ".csv");
            String line = content.substring(2500, 2530);
        }
        System.out.print("Large file read all then find line 2500: ");
        System.out.println(System.currentTimeMillis() - time);


        // test reading certain line in file
        time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            String rRwo = FileUtil.readFile(NUM_ROWS + ".csv", 2500, 2530);
        }
        System.out.print("Large file read line 2500: ");
        System.out.println(System.currentTimeMillis() - time);
    }

    @Test
    void find_sweet_spot() throws IOException {

        /*
            Test for 500,000 rows, 2 partitions vs one file:
                reading one row from partition: 19
                reading one row from large file: 17

            After more tests found them to be equal times
         */

        int NUM_ROWS = 50000;
        int ITERATIONS = 1000;
        int PARTITIONS = 2;

        // make NUM_ROWS / PARTITIONS test files
        String row = "$450,02/18/2020";
        int rowsPerPartition = NUM_ROWS / PARTITIONS;
        for (int p = 1; p <= PARTITIONS; p++) {
            String rows = "";
            for (int i = 0; i < rowsPerPartition; i++) {
                rows += i + "," + row + "\n";
            }
            System.out.println("p"+p);
            FileUtil.writeFile("partition_" + p + ".csv", rows);
        }

        long time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            String rRow = FileUtil.readFile("partition_1.csv", 2500, 2530);
        }
        System.out.print("reading one row from partition: ");
        System.out.println(System.currentTimeMillis() - time);



        String rows = "";
        for (int i = 0; i < NUM_ROWS; i++) {
            rows += i + "," + row + "\n";
            if (i == NUM_ROWS/PARTITIONS)
                System.out.println("half");
        }
        FileUtil.writeFile(NUM_ROWS + ".csv", rows);

        time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            String rRwo = FileUtil.readFile(NUM_ROWS + ".csv", 2500, 2530);
        }
        System.out.print("reading one row from large file: ");
        System.out.println(System.currentTimeMillis() - time);

    }

    @Test
    void readAllLinesSplit_vs_randomAccessRow_vs_readRowsFromDifferentFiles() throws IOException {

        /*
            TEST RESULTS:
                reading 500000 rows: 43
                reading one row 3000 times: 43
                Large File more efficient until size: 166


                reading 500000 rows: 22
                reading one row 3000 times: 44
                Large File more efficient until size: 166

            This is reading a large file and doing a string split by '\n' char. (probably faster that row reads)

         */

        for (int j = 0; j < 1; j++) {

            int NUM_ROWS = 500000;
            int SINGLE_FILE_READS = 3000;

            // large file
            String row = "$450,02/18/2020";
            StringBuilder rows = new StringBuilder();
            for (int i = 0; i < NUM_ROWS; i++) {
                rows.append(i).append(",").append(row).append("\n");
            }
            FileUtil.writeFile(NUM_ROWS + ".csv", rows.toString());

            long time = System.currentTimeMillis();
            String allRows = FileUtil.readFile(NUM_ROWS + ".csv");
            String[] rowsSplit = allRows.split("\n");
            System.out.print("reading " + NUM_ROWS + " rows: ");
            System.out.println(System.currentTimeMillis() - time);

            long rtime = System.currentTimeMillis();
            String rRow = FileUtil.readLine(NUM_ROWS + ".csv", NUM_ROWS/2);
            System.out.print("reading one line from large file: ");
            System.out.println(System.currentTimeMillis() - rtime);



            FileUtil.writeFile("oneRow0.csv",   "0," + row + "\n");
            FileUtil.writeFile("oneRow1.csv",   "0," + row + "\n");
            FileUtil.writeFile("oneRow2.csv",   "0," + row + "\n");
            long stime = System.currentTimeMillis();
            for (int i = 0; i < SINGLE_FILE_READS; i++) {
                int index = i % 3;
                String rRwo = FileUtil.readFile("oneRow" + index + ".csv");
            }
            System.out.print("reading one row " + SINGLE_FILE_READS + " times: ");
            System.out.println(System.currentTimeMillis() - stime);

            System.out.println("Large File more efficient until size: " + NUM_ROWS/SINGLE_FILE_READS);
        }


    }

    @Test
    void mkDir_vs_mkFile() throws IOException {

        /*
            TEST RESULTS:
                making file: 11
                making directory: 19

            making an empty file is faster than a directory

         */

        int ITERATIONS = 10000;

        long time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {

            File file = new File("test.txt");
            file.createNewFile();
        }
        System.out.print("making file: ");
        System.out.println(System.currentTimeMillis() - time);


        time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            File directory = new File("test");
            directory.mkdirs();
        }
        System.out.print("making directory: ");
        System.out.println(System.currentTimeMillis() - time);



    }

    @Test
    void readFileName_vs_readFileLine() throws IOException {
        System.out.println(Long.MAX_VALUE);

        /*
            TEST RESULTS:
                read file name: 111
                read file row: 117

            reading a file name is slightly faster

         */

        File directory = new File("one_rows");
        directory.mkdirs();
        FileUtil.writeFile("one_rows/1_row.csv", "25");

        int ITERATIONS = 10000;


        long time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            String name = new File("one_rows").listFiles()[0].getName();
        }
        System.out.print("read file name: ");
        System.out.println(System.currentTimeMillis() - time);



        time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            String name = FileUtil.readFile("one_rows/1_row.csv");
        }
        System.out.print("read file row: ");
        System.out.println(System.currentTimeMillis() - time);



    }

    @Test
    void readSingleLineIndices_vs_oneFileWithAllIndices() throws IOException {
        /*
            TEST RESULTS:
                read each single line index: 2372
                read all lines index: 5

            reading all lines is a lot faster than reading individual line files
         */


        int ITERATIONS = 50000;

        File directory = new File("one_rows");
        directory.mkdirs();
        for (int i = 0; i < ITERATIONS; i++) {
            FileUtil.writeFile("one_rows/single_line_index_" + i + ".index", "25");
        }
        for (int i = 0; i < ITERATIONS; i++) {
            FileUtil.writeBytes("one_rows/all_lines_index.index", i * 8, TypeToByteUtil.longToByteArray(25L));
        }


        long time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] bytes = FileUtil.readBytes("one_rows/single_line_index_" + i + ".index");
        }
        System.out.print("read each single line index: ");
        System.out.println(System.currentTimeMillis() - time);


        time = System.currentTimeMillis();
        byte[] bytes = FileUtil.readBytes("one_rows/all_lines_index.index");
        System.out.print("read all lines index: ");
        System.out.println(System.currentTimeMillis() - time);
    }

    @Test
    void bulkReadBytes_vs_individualReads() throws IOException {
         /*
            TEST RESULTS (iterations 50000):
                num ITERATIONS individual reads: 414
                num ITERATIONS bulk read: 71
                read all lines index: 1

            bulk reads are much faster. But reading all lines is even faster.
         */


        int ITERATIONS = 100000;

        File directory = new File("one_rows");
        directory.mkdirs();
        byte[] file = new byte[ITERATIONS * 8];
        FileUtil.writeBytes("one_rows/all_lines_index.index", 0, file);


        long time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] bytes = FileUtil.readBytes("one_rows/all_lines_index.index", i * 8, 8);
        }
        System.out.print("num ITERATIONS individual reads: ");
        System.out.println(System.currentTimeMillis() - time);


        List<Long> startBytes = new ArrayList<>();
        List<Long> numBytes = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            startBytes.add((long) (i * 8));
            numBytes.add(8L);
        }
        time = System.currentTimeMillis();
        List<Object> bytes = FileUtil.readBytes("one_rows/all_lines_index.index", startBytes, numBytes);
        System.out.print("num ITERATIONS bulk read: ");
        System.out.println(System.currentTimeMillis() - time);
    }

    @Test
    void bulkReadBytes_vs_allBytes() throws IOException {
         /*
            TEST RESULTS:

                (ITERATIONS 1000000, MULT 100, BULK_LINES 1000)
                    num BULK_LINES bulk read: 140
                    read all lines index: 198

                (ITERATIONS 1000, MULT 1000, BULK_LINES 1)
                    num BULK_LINES bulk read: 11
                    read all lines index: 13
         */

        int ITERATIONS = 1000;
        int MULT = 1000;
        int BULK_LINES = 1;

        File directory = new File("one_rows");
        directory.mkdirs();
        FileUtil.deleteFile("one_rows/all_lines_index.index");
        byte[] file = new byte[ITERATIONS * 8];
        FileUtil.writeBytes("one_rows/all_lines_index.index", 0, file);


        List<Long> startBytes = new ArrayList<>();
        List<Long> numBytes = new ArrayList<>();
        for (int i = 0; i < BULK_LINES; i++) {
            startBytes.add((long) (i * 8));
            numBytes.add(8L);
        }
        long time = System.currentTimeMillis();
        for (int i = 0; i <MULT; i++) {
            List<Object> bytes = FileUtil.readBytes("one_rows/all_lines_index.index", startBytes, numBytes);
//            int size = bytes.stream()
//                    .map( byteArr -> byteArr.length )
//                    .reduce(0, Integer::sum);
//            System.out.println(size);
        }
        System.out.print("num BULK_LINES bulk read: ");
        System.out.println(System.currentTimeMillis() - time);


        time = System.currentTimeMillis();
        for (int i = 0; i <MULT; i++) {
            byte[] allLines = FileUtil.readBytes("one_rows/all_lines_index.index");
//            System.out.println(allLines.length);
        }
        System.out.print("read all lines index: ");
        System.out.println(System.currentTimeMillis() - time);
    }

    @Test
    void calculateRoute_vs_readRoutes() {
        /*
                Route bytes could be: (budget 8 - 12 bytes)
                    - [8 bytes offset][4 bytes length]
                    - [6 bytes offset][4 bytes length]



         */


        byte[] bytes = TypeToByteUtil.longToByteArray(25L);
        long lVal = TypeToByteUtil.byteArrayToLong(bytes);
        System.out.println(lVal);


    }

    @Test
    void writeLinesToSeperateFiles_vs_writeLinesToOne() throws IOException {
        /*
            TEST RESULTS:
                writing to NUM_ROWS files: 2896
                writing to 1 files: 8

         */

        File directory = new File("test");
        directory.mkdirs();
        String row = "$450,02/18/2020";

        int ITERATIONS = 100;
        int NUM_ROWS = 100;

        for (int i = 0; i < NUM_ROWS; i++) {
            FileUtil.createFile("test/" + i + ".csv");
        }


        // write NUM_ROWS lines to NUM_ROWS files
        long time = System.currentTimeMillis();
        for (int j = 0; j < ITERATIONS; j++) {
            for (int i = 0; i < NUM_ROWS; i++) {
                FileUtil.writeBytes("test/" + i + ".csv", 0, (i + "," + row).getBytes(StandardCharsets.UTF_8) );
            }
        }
        System.out.print("writing to NUM_ROWS files: ");
        System.out.println(System.currentTimeMillis() - time);


        // write NUM_ROWS lines to 1 file
        time = System.currentTimeMillis();
        for (int j = 0; j < ITERATIONS; j++) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < NUM_ROWS; i++) {
                stringBuilder.append(row).append("\n");
            }
            FileUtil.writeBytes("test/" + 0 + ".csv", 0, stringBuilder.toString().getBytes(StandardCharsets.UTF_8) );
        }
        System.out.print("writing to 1 files: ");
        System.out.println(System.currentTimeMillis() - time);

    }

    @Test
    void writeLinesToSeperateFiles_vs_writeLinesToOneJumping() throws IOException {
        /*
            TEST RESULTS:
                writing to NUM_ROWS files: 61
                writing to 1 file jumping: 78

            jumping in the same file is just as bad as jumping to other files

         */

        File directory = new File("test");
        directory.mkdirs();
        String row = "$450,02/18/2020 \n";

        int ITERATIONS = 100;
        int NUM_ROWS = 10;
        int LARGE_FILE_ROWS = 1000;



        // write NUM_ROWS lines to NUM_ROWS files
        for (int i = 0; i < NUM_ROWS; i++) {
            FileUtil.createFile("test/" + i + ".csv");
        }
        long time = System.currentTimeMillis();
        for (int j = 0; j < ITERATIONS; j++) {
            for (int i = 0; i < NUM_ROWS; i++) {
                FileUtil.writeBytes("test/" + i + ".csv", 0, (i + "," + row).getBytes(StandardCharsets.UTF_8) );
            }
        }
        System.out.print("writing to NUM_ROWS files: ");
        System.out.println(System.currentTimeMillis() - time);


        // write NUM_ROWS lines to 1 file jumping
        byte[] bytes = (1 + "," + row).getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < LARGE_FILE_ROWS; i++) {
            FileUtil.writeBytes("test/" + 0 + ".csv", (long) i * bytes.length, bytes);
        }
        int step = LARGE_FILE_ROWS/NUM_ROWS;
        time = System.currentTimeMillis();
        for (int j = 0; j < ITERATIONS; j++) {
            for (int i = 0; i < NUM_ROWS; i++) {
                FileUtil.writeBytes("test/" + 0 + ".csv", step * i, (i + "," + row).getBytes(StandardCharsets.UTF_8) );
            }
        }
        System.out.print("writing to 1 file jumping: ");
        System.out.println(System.currentTimeMillis() - time);
    }

    @Test
    void writeComplexObjectToFile() throws IOException {
//        RollbackRecord rollbackRecord = new RollbackRecord(
//            List.of(
//                new RowWritePackage(
//                    new IndexRoute("Order", 123456L, 123),
//                    new Row(new HashMap<>(), "Order"),
//                    new byte[150]
//                )
//            ),
//            Map.of(
//                "some path",
//                List.of(
//                    new IndexWritePackage(
//                        new IndexRoute("Order", 123456L, 123),
//                        null,
//                        Date.class,
//                        null,
//                        Date.of("2023-08-01", LocalDate.class),
//                        "some path"
//                    )
//                )
//            ),
//            List.of(
//                new IndexRoute("Order", 123456L, 123)
//            )
//        );

//        Timer timer = Timer.start();
//        FileUtil.writeObjectToFile("test.obj", rollbackRecord);
//        timer.printRestart();
//
//        timer = Timer.start();
//        rollbackRecord = FileUtil.readObjectFromFile("test.obj", RollbackRecord.class);
//        timer.printRestart();
//
//        System.out.println(rollbackRecord);


//        String serialized = rollbackRecord.toString();
//
//        Timer timer = Timer.start();
//        FileUtil.writeBytes("test.obj", 0, serialized.getBytes());
//        timer.printRestart();
//
//        System.out.println(rollbackRecord);

    }

    @Test
    void listSubfolders_vs_readLarge() throws IOException {
        /*
            TEST RESULTS (iterations 50000):
                reading large file: 420
                sub folders: 184

            reading sub folders is faster
         */


        int ITERATIONS = 10000;

        File directory = new File("one_rows");
        directory.mkdirs();
        byte[] file = new byte[ITERATIONS * 8];
        FileUtil.writeBytes("one_rows/all_lines_index.index", 0, file);
        int fileSize = Math.toIntExact(FileUtil.fileSize("one_rows/all_lines_index.index"));


        long time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] bytes = FileUtil.readBytes("one_rows/all_lines_index.index", 0L, fileSize);
        }
        System.out.print("reading large file: ");
        System.out.println(System.currentTimeMillis() - time);



        time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            List<File> folders = FileUtil.getSubFolders("src");
        }
        System.out.print("sub folders: ");
        System.out.println(System.currentTimeMillis() - time);
    }

    @Test
    void listSubFoldersRecursive_speed_test() throws IOException {


        Timer timer = Timer.start();
        FileUtil.getLeafFolders("db/Order/indices_Order");
        timer.printRestart("leaf");


        timer = Timer.start();
        List<File> dirs =  FileUtil.getSubFoldersRecursive("db/Order/indices_Order/time");
        timer.printRestart(String.valueOf(dirs.size()));


        timer = Timer.start();
        FileUtil.getSubFoldersRecursive("db/Order/indices_Order/orderId");
        timer.printRestart();


        timer = Timer.start();
        FileUtil.getSubFoldersRecursive("db/Order/indices_Order/total");
        timer.printRestart();


        File directory = new File("one_rows");
        directory.mkdirs();
        byte[] file = new byte[1000 * 8];


        timer = Timer.start();
        FileUtil.writeBytes("one_rows/all_lines_index.index", 0, file);
        timer.printRestart("writing directories to file");

        timer = Timer.start();
        byte[] bytes = FileUtil.readBytes("one_rows/all_lines_index.index", 0L, file.length);
        timer.printRestart("reading directories from file");

    }

    @Test
    void listSubfolders_vs_readCountFile() throws IOException {
        /*
            TEST RESULTS (iterations 100) getting 1000 subFiles in dir:
                reading count file: 1
                sub folders: 468
                sub folders 2: 67
                sub folders 3: 58

            reading sub folders is faster
         */


        int ITERATIONS = 100;

        File directory = new File("one_rows");
        directory.mkdirs();
        byte[] file = new byte[8];
        FileUtil.writeBytes("one_rows/all_lines_index.index", 0, file);
        int fileSize = Math.toIntExact(FileUtil.fileSize("one_rows/all_lines_index.index"));


        long time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] bytes = FileUtil.readBytes("one_rows/all_lines_index.index", 0L, 8);
        }
        System.out.print("reading count file: ");
        System.out.println(System.currentTimeMillis() - time);



        time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            FileUtil.getSubFiles("db/Order/META_Order/orderId").size();
//            System.out.print("");
        }
        System.out.print("sub folders: ");
        System.out.println(System.currentTimeMillis() - time);




        time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            long fileCount = Files.list(Paths.get("db/Order/META_Order/orderId")).count();
//            System.out.print("");
        }
        System.out.print("sub folders 2: ");
        System.out.println(System.currentTimeMillis() - time);




        time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            Path dir = Paths.get("db/Order/META_Order/orderId");
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
//                long count = StreamSupport.stream( stream.spliterator(), true ).count();
//                long count = stream.spliterator().estimateSize();
//                System.out.println(count);
                int count = 0;
                for (Path entry : stream) {
                    count++;
                }
            }
        }
        System.out.print("sub folders 3: ");
        System.out.println(System.currentTimeMillis() - time);

    }

    @Test
    void getFileSize_vs_first8Bytes() throws IOException {
        /*
            TEST RESULTS:
                reading 8 bytes: 70
                reading file size: 20
         */

        int ITERATIONS = 10000;

        File directory = new File("one_rows");
        directory.mkdirs();
        byte[] file = new byte[100000 * 10];
        FileUtil.writeBytes("one_rows/all_lines_index.index", 0, file);


        long time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            byte[] bytes = FileUtil.readBytes("one_rows/all_lines_index.index", 0L, 8);
        }
        System.out.print("reading 8 bytes: ");
        System.out.println(System.currentTimeMillis() - time);


        time = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            long fileSize = FileUtil.fileSize("one_rows/all_lines_index.index");
        }
        System.out.print("reading file size: ");
        System.out.println(System.currentTimeMillis() - time);
    }

}
