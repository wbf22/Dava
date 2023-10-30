package org.dava.core.systemtests;

import org.dava.common.logger.Logger;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;

class DirectoryMeasurementsTest {

    Logger log = Logger.getLogger("log");

    @Test
    void mesaure_directory() {
        String DIRECTORY = "/Users/brandon.fowler/Desktop/db";
//        String DIRECTORY = "db";


        getSize(DIRECTORY, 0);

        log.space();
        log.print(DIRECTORY + "/Order/Order.csv " + FileUtil.fileSize(DIRECTORY + "/Order/Order.csv")/1000.0 + " mb");

    }


    long getSize(String directory, int depth) {
        File[] files = FileUtil.listFiles(directory);
        long size = Arrays.stream(files).sequential()
            .filter(file -> !file.isDirectory())
            .map(File::getPath)
            .map(FileUtil::fileSize)
            .reduce(0L, Long::sum);

        size += Arrays.stream(files).sequential()
            .filter(File::isDirectory)
            .map(File::getPath)
            .map(path -> getSize(path, depth + 1))
            .reduce(0L, Long::sum);

        log.print(" ".repeat(depth) + directory + ": " + size/1000.0 + " mb");

        return size;
    }
}
