package org.dava.core.speedtests;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Arrays;

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
     *  - making an empty file is faster than a empty directory
     *  - reading a file name is slightly faster than reading a one line file
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
    void rows_read_per_file_read() throws IOException {

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
            String rRow = FileUtil.readLineFromFile(NUM_ROWS + ".csv", NUM_ROWS/2, true);
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


}
