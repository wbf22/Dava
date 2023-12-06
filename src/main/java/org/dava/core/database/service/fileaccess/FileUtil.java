package org.dava.core.database.service.fileaccess;

import org.dava.core.database.service.objects.WritePackage;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class FileUtil {

    public static Cache cache = new Cache();




    private FileUtil() {}


    public static void invalidateCache() {
        cache.invalidateCacheAll();
    }


    public static void writeObjectToFile(String destinationPath, Object object) throws IOException {
        try ( ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(destinationPath)) ) {
            oos.writeObject(object);
        }

        cache.invalidateCacheAll(); // for directory listing
    }

    public static <T> T readObjectFromFile(String filePath, Class<T> objectType) throws IOException {

        return cache.get(filePath, Cache.hash("readObjectFromFile", objectType), () -> {
            T object = null;
            try ( ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath)); ) {
                Object obj = ois.readObject();
                if (objectType.isInstance(obj)) {
                    object = objectType.cast(obj);
                }

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return object;
        });
    }


    public static String readFile(String filePath) throws IOException {
        return cache.get(filePath, Cache.hash("readFile"), () ->
            new String(readBytes(filePath), StandardCharsets.UTF_8)
        );
    }

    public static byte[] readBytes(String filePath) throws IOException {

        return cache.get(filePath, Cache.hash("readBytes"), () -> {
            File file = new File(filePath);

            FileInputStream fileInputStream = new FileInputStream(file);

            byte[] fileContent = fileInputStream.readAllBytes();
            fileInputStream.close();

            return fileContent;
        });
    }

    public static String readFile(String filePath, long startByte, int numBytes) throws IOException {

        return cache.get(filePath, Cache.hash("readFile", startByte, numBytes), () -> {
            byte[] bytes = readBytes(filePath, startByte, numBytes);
            return (bytes != null)? new String(bytes, StandardCharsets.UTF_8) : null;
        });
    }

    public static byte[] readBytes(String filePath, long startByte, Integer numBytes) throws IOException {
        return cache.get(filePath, Cache.hash("readBytes", startByte, numBytes), () -> {
            try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
                raf.seek(startByte); // Set the file pointer to the desired position

                int bytesToRead = (numBytes == null)? (int) raf.length() : numBytes;
                byte[] buffer = new byte[bytesToRead];
                int bytesRead = raf.read(buffer); // Read the specified number of bytes

                return (bytesRead != -1)? buffer : null;
            }
        });

    }

    public static List<Object> readBytes(String filePath, List<Long> startBytes, List<Long> numBytes) throws IOException {

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {

            List<Object> reads = new ArrayList<>();
            for (int i = 0; i < startBytes.size(); i++) {
                raf.seek( startBytes.get(i) ); // Set the file pointer to the desired position

                Long num = numBytes.get(i);

                // read bytes going through cache
                Object readBytes = cache.get(filePath, Cache.hash("readBytes", startBytes.get(i), num), () -> {

                    byte[] buffer = new byte[ Math.toIntExact(num) ];
                    int bytesRead = raf.read(buffer); // Read the specified number of bytes
                    return (bytesRead != -1)? buffer : null;

                });

                reads.add(
                    readBytes
                );
            }

            return reads;
        }


    }

    public static String readLine(String filePath, long startByte) throws IOException {
        return cache.get(filePath, Cache.hash("readLine", startByte), () -> {
            RandomAccessFile raf = new RandomAccessFile(filePath, "r");
            raf.seek(startByte); // Set the file pointer to the desired position

            String data = raf.readLine();

            raf.close();

            return data;
        });
    }

    public static void writeFile(String desitnationPath, String fileContents) throws IOException {

        FileOutputStream fileOutputStream;
        fileOutputStream = new FileOutputStream(desitnationPath);

        fileOutputStream.write(fileContents.getBytes(StandardCharsets.UTF_8));
        fileOutputStream.close();

        cache.invalidate(desitnationPath);
        cache.invalidateCacheAll(); // for directory listing
    }

    public static void writeBytes(String filePath, long position, byte[] data) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            // Move to the desired position in the file
            file.seek(position);

            // Write data at the current position
            file.write(data);
        }

        cache.invalidate(filePath);
    }

    public static void changeCount(String filePath, long position, long change, int countByteLength) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            // Move to the desired position in the file
            file.seek(position);

            // read the current count
            byte[] buffer = new byte[countByteLength];
            int bytesRead = file.read(buffer);

            long newCount = TypeToByteUtil.byteArrayToLong(buffer) + change;

            byte[] newCountbytes = TypeToByteUtil.longToByteArray(newCount);

            // Write data at the current position
            file.seek(position);
            file.write(newCountbytes);
        }

        cache.invalidate(filePath);
    }

    /**
     * writes the writePackages.
     * Also updates null offsets in packages to the end of the table at insert
     */
    public static void writeBytes(String filePath, List<WritePackage> writePackages) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            writePackages.forEach( writePackage -> {
                    try {
                        // Move to the desired position in the file
                        long offset = (writePackage.getOffsetInTable() == null)? file.length() : writePackage.getOffsetInTable();
                        file.seek(offset);

                        // Write data at the current position
                        file.write( writePackage.getData() );

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        }

        cache.invalidate(filePath);
    }


    /**
     * writes the writePackages if they are within the length of the file
     * Also updates null offsets in packages to the end of the table at insert
     */
    public static void writeBytesIfPossible(String filePath, List<WritePackage> writePackages) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            long fileSize = file.length();

            writePackages.forEach( writePackage -> {
                try {
                    // Move to the desired position in the file
                    long offset = (writePackage.getOffsetInTable() == null)? file.length() : writePackage.getOffsetInTable();

                    if (offset < fileSize) {
                        file.seek(offset);

                        // Write data at the current position
                        file.write( writePackage.getData() );
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        cache.invalidate(filePath);
    }

    public static void writeFileAppend(String filePath, String data) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.seek(file.length());
            file.write(data.getBytes());
        }

        cache.invalidate(filePath);
    }

    public static void writeBytesAppend(String filePath, byte[] data) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.seek(file.length());
            file.write(data);
        }

        cache.invalidate(filePath);
    }

    public static void replaceFile(String filePath, byte[] data) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength( 0 );
            file.write(data);
        }

        cache.invalidate(filePath);
    }

    public static void replaceFile(String filePath, List<WritePackage> writePackages) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength( 0 );
            writePackages.forEach( writePackage -> {
                try {
                    // Move to the desired position in the file
                    long offset = (writePackage.getOffsetInTable() == null)? file.length() : writePackage.getOffsetInTable();
                    file.seek(offset);

                    // Write data at the current position
                    file.write( writePackage.getData() );

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        cache.invalidate(filePath);
    }

    public static void truncate(String filePath, Long newSizeInBytes) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength( newSizeInBytes );
        }

        cache.invalidate(filePath);
    }

    public static void createDirectoriesIfNotExist(String directoryPath) throws IOException {

        Path path = Paths.get(directoryPath);
        Files.createDirectories(path);

        cache.invalidateCacheAll(); // for directory listing
    }

    public static void moveFilesToDirectory(List<File> sourceFiles, String destinationDirectory) throws IOException {

        File destinationDir = new File(destinationDirectory);
        if (!destinationDir.exists()) {
            boolean created = destinationDir.mkdirs();
            if (!created) {
                throw new IOException("DAVA Failed to create directory: " + destinationDirectory);
            }
        }

        for (File sourceFile : sourceFiles) {
            Path sourcePath = sourceFile.toPath();
            Path destinationPath = destinationDir.toPath().resolve(sourceFile.getName());

            Files.move(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
        }

        cache.invalidateCacheAll(); // for directory listing
    }

    public static void copyFilesToDirectory(List<File> sourceFiles, String destinationDirectory) throws IOException {

        File destinationDir = new File(destinationDirectory);
        if (!destinationDir.exists()) {
            boolean created = destinationDir.mkdirs();
            if (!created) {
                throw new IOException("DAVA Failed to create directory: " + destinationDirectory);
            }
        }

        for (File sourceFile : sourceFiles) {
            Path sourcePath = sourceFile.toPath();
            Path destinationPath = destinationDir.toPath().resolve(sourceFile.getName());

            Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
        }

        cache.invalidateCacheAll(); // for directory listing
    }

    public static boolean createFile(String filePath) throws IOException {
        cache.invalidateCacheAll(); // for directory listing
        return new File(filePath).createNewFile();
    }

    public static boolean createFile(String filePath, byte[] content) throws IOException {

        File newFile = new File(filePath);
        boolean success = newFile.createNewFile();
        writeBytes(filePath, 0, content);


        cache.invalidateCacheAll(); // for directory listing
        return success;
    }

    public static boolean renameFile(String oldFilePath, String newFilePath) {

        File oldFile = new File(oldFilePath);
        File newFile = new File(newFilePath);

        boolean success = false;
        if (oldFile.exists()) {
            success = oldFile.renameTo(newFile);
        }

        cache.invalidateCacheAll(); // for directory listing
        return success;
    }

    public static boolean exists(String filePath) {
        return cache.get(filePath, Cache.hash("exists"), () -> {
            File file = new File(filePath);
            return file.exists();
        });
    }

    public static Long fileSize(String filePath) {
        return cache.get(filePath, Cache.hash("fileSize"), () -> {
            File file = new File(filePath);
            return file.length();
        });
    }

    public static long popBytes(String filePath, int bytesToPop, List<Long> startBytes) throws IOException { //, boolean setFirst8BytesAsSizeOfThisFile

        File file = new File(filePath);
        startBytes.sort(Long::compareTo);

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {

            while (!startBytes.isEmpty()) {
                // pop empties off the bottom of the file, if they're on the list
                long startByteToRemove = startBytes.get(startBytes.size() - 1);
                startBytes = startBytes.subList(0, startBytes.size() - 1);

                if ( startByteToRemove == raf.length() - bytesToPop) {
                    raf.setLength(raf.length() - bytesToPop);
                }
                else {
                    // otherwise switch with last row and pop.

                    // get lastRow
                    long endStartByte = raf.length() - bytesToPop;
                    raf.seek(endStartByte);
                    byte[] endBuffer = new byte[bytesToPop];
                    int endBytesRead = raf.read(endBuffer);
                    byte[] endData = (endBytesRead != -1)? endBuffer : null;

                    // switch the two
                    raf.seek(startByteToRemove);
                    raf.write(endData);

                    // truncate the end bytes off the file
                    raf.setLength(raf.length() - bytesToPop);
                }

            }

            cache.invalidate(filePath);
            return raf.length();
        }
    }

    public static boolean deleteFile(String filePath) throws IOException {

        boolean success = false;
        if (exists(filePath)) {
            Path path = Paths.get(filePath);
            if (!Files.isDirectory(path)) {
                Files.delete(path);
                success = true;
            }
        }

        cache.invalidateCacheAll(); // for directory listing
        return success;
    }

    public static boolean deleteFile(File file) throws IOException {

        boolean success = file.delete();

        cache.invalidateCacheAll(); // for directory listing
        return success;
    }

    public static void deleteDirectory(String directoryPath) throws IOException {

        Path path = Paths.get(directoryPath);
        Files.walkFileTree(path, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new FileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                Files.delete(path); // Delete files
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
                if (path != null) {
                    Files.delete(path); // Delete directories after their contents
                    return FileVisitResult.CONTINUE;
                } else {
                    throw e;
                }
            }
            @Override
            public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes basicFileAttributes) { return FileVisitResult.CONTINUE; }
            @Override
            public FileVisitResult visitFileFailed(Path path, IOException e) { return FileVisitResult.CONTINUE; }
        });
        cache.invalidateCacheAll(); // for directory listing
    }



    /*
           TODO action below require us to invalidate the entire cache in some of the methods above,
                decide if we should be smarter about which cache entries to invalidate.
     */

    public static File[] listFiles(String path) {
        return cache.get(path, Cache.hash("listFiles"), () ->
            new File(path).listFiles()
        );
    }

    public static List<File> getSubFolders(String path) {
        return cache.get(path, Cache.hash("getSubFolders"), () -> {
            File[] files = FileUtil.listFiles(path);
            if (files != null && files.length > 0) {
                return Arrays.stream(files)
                    .filter(File::isDirectory)
                    .toList();
            }
            return new ArrayList<>();
        });
    }

    public static List<File> getSubFiles(String path) {
        return cache.get(path, Cache.hash("getSubFiles"), () -> {
            File[] files = FileUtil.listFiles(path);
            if (files != null && files.length > 0) {
                return Arrays.stream(files)
                    .filter(File::isFile)
                    .toList();
            }
            return new ArrayList<>();
        });
    }

    public static List<File> getLeafFolders(String directory) {
        List<File> leaves = new ArrayList<>();
        List<File> files = List.of(new File(directory));
        do {
            files = files.parallelStream()
                .flatMap( file -> {
                    List<File> subFiles = Arrays.stream(
                        cache.get(file.getPath(), Cache.hash("listFiles"), () ->
                            file.listFiles()
                        )
                    ).filter(File::isDirectory)
                    .toList();

                    if (subFiles.isEmpty()) {
                        leaves.add(file);
                    }
                    return subFiles.stream();
                })
                .toList();

        } while (!files.isEmpty());

        return leaves;
    }

    public static List<File> getSubFoldersRecursive(String directory) {

        List<File> newFiles = Arrays.stream(
            cache.get(directory, Cache.hash("listFiles"), () ->
                new File(directory).listFiles()
            )
        ).filter(File::isDirectory)
        .toList();

        List<File> files = new ArrayList<>(newFiles);

        while(!newFiles.isEmpty()) {
            newFiles = newFiles.parallelStream()
                .flatMap(file ->
                    Arrays.stream(
                        cache.get(file.getPath(), Cache.hash("listFiles"), () ->
                            file.listFiles()
                        )
                    )
                )
                .filter(File::isDirectory)
                .toList();

            files.addAll(newFiles);
        }

        return files;
    }

    public static long getSubFilesAndFolderCount(String directory) throws IOException {
        return cache.get(directory, Cache.hash("getSubFilesAndFolderCount"), () -> {
            Path dir = Paths.get(directory);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
                int count = 0;
                for (Path ignored : stream) {
                    count++;
                }
                return count;
            }
        });


    }


}
