package org.dava.core.database.service.fileaccess;

import org.dava.core.database.service.objects.RowWritePackage;
import org.dava.core.database.service.objects.WritePackage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.IntStream;

public class FileUtil {

    private FileUtil() {}



    public static void writeObjectToFile(String destinationPath, Object object) throws IOException {
        try ( ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(destinationPath)) ) {
            oos.writeObject(object);
        }
    }

    public static <T> T readObjectFromFile(String filePath, Class<T> objectType) throws IOException {
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
    }


    public static String readFile(String filePath) throws IOException {
        return new String(readBytes(filePath), StandardCharsets.UTF_8);
    }

    public static byte[] readBytes(String filePath) throws IOException {
        File file = new File(filePath);

        FileInputStream fileInputStream = new FileInputStream(file);

        byte[] fileContent = fileInputStream.readAllBytes();
        fileInputStream.close();

        return fileContent;
    }

    public static String readFile(String filePath, long startByte, int numBytes) throws IOException {
        byte[] bytes = readBytes(filePath, startByte, numBytes);
        return (bytes != null)? new String(bytes, StandardCharsets.UTF_8) : null;
    }

    public static byte[] readBytes(String filePath, long startByte, int numBytes) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(startByte); // Set the file pointer to the desired position

            byte[] buffer = new byte[numBytes];
            int bytesRead = raf.read(buffer); // Read the specified number of bytes

            return (bytesRead != -1)? buffer : null;
        }
    }

    public static List<Object> readBytes(String filePath, List<Long> startBytes, List<Long> numBytes) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {

            List<Object> reads = new ArrayList<>();
            for (int i = 0; i < startBytes.size(); i++) {
                raf.seek( startBytes.get(i) ); // Set the file pointer to the desired position

                byte[] buffer = new byte[ Math.toIntExact( numBytes.get(i) ) ];
                int bytesRead = raf.read(buffer); // Read the specified number of bytes

//                Byte[] read = new Byte[buffer.length];
//                IntStream.range(0, buffer.length)
//                        .forEach(j -> read[j] = buffer[j]);

                reads.add(
                        (bytesRead != -1)? buffer : null
                );
            }

            return reads;
        }
    }

    public static String readLine(String filePath, long startByte) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        raf.seek(startByte); // Set the file pointer to the desired position

        String data = raf.readLine();

        raf.close();

        return data;
    }

    public static void writeFile(String desitnationPath, String fileContents) throws IOException {
        FileOutputStream fileOutputStream;
        fileOutputStream = new FileOutputStream(desitnationPath);

        fileOutputStream.write(fileContents.getBytes(StandardCharsets.UTF_8));
        fileOutputStream.close();
    }

    public static void writeFile(String filePath, long position, String data) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            // Move to the desired position in the file
            file.seek(position);

            // Write data at the current position
            file.write(data.getBytes());
        }
    }

    public static void writeBytes(String filePath, long position, byte[] data) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            // Move to the desired position in the file
            file.seek(position);

            // Write data at the current position
            file.write(data);
        }
    }

    /**
     * writes the writePackages.
     *
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
    }

    public static void writeFileAppend(String filePath, String data) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.seek(file.length());
            file.write(data.getBytes());
        }
    }

    public static void writeBytesAppend(String filePath, byte[] data) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.seek(file.length());
            file.write(data);
        }
    }

    public static void replaceFile(String filePath, byte[] data) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength( 0 );
            file.write(data);
        }
    }

    public static void createParentFolderIfNotExist(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        Files.createDirectories(path.getParent());
    }

    public static void createDirectoriesIfNotExist(String directoryPath) throws IOException {
        Path path = Paths.get(directoryPath);
        Files.createDirectories(path);
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
    }

    public static boolean createFile(String filePath) throws IOException {
        return new File(filePath).createNewFile();
    }

    public static boolean createFile(String filePath, byte[] content) throws IOException {
        File newFile = new File(filePath);
        boolean success = newFile.createNewFile();
        writeBytes(filePath, 0, content);

        return success;
    }

    public static boolean renameFile(String oldFilePath, String newFilePath) {
        File oldFile = new File(oldFilePath);
        File newFile = new File(newFilePath);

        if (oldFile.exists()) {
            return oldFile.renameTo(newFile);
        }
        return false;
    }

    public static File[] listFiles(String path) {
        return new File(path).listFiles();
    }

    public static List<File> getSubFolders(String path) {
        File[] files = FileUtil.listFiles(path);
        if (files != null && files.length > 0) {
            return Arrays.stream(files)
                .filter(File::isDirectory)
                .toList();
        }
        return new ArrayList<>();
    }

    public static List<File> getLeafFolders(String directory) {
        List<File> leaves = new ArrayList<>();

        List<File> files = List.of(new File(directory));
        do {
            files = files.parallelStream()
                .flatMap( file -> {
                    List<File> subFiles = Arrays.stream(file.listFiles())
                        .filter(File::isDirectory)
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

        List<File> newFiles = Arrays.stream(new File(directory).listFiles())
            .filter(File::isDirectory)
            .toList();

        List<File> files = new ArrayList<>(newFiles);

        while(!newFiles.isEmpty()) {
            newFiles = newFiles.parallelStream()
                .flatMap(file -> Arrays.stream(file.listFiles()))
                .filter(File::isDirectory)
                .toList();

            files.addAll(newFiles);
        }

        return files;
    }

    public static boolean exists(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    public static Long fileSize(String filePath) {
        File file = new File(filePath);
        return file.length();
    }

    public static void popBytes(String filePath, int bytesToPop, List<Long> startBytes) throws IOException {
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
        }
    }

    public static boolean deleteFile(String filePath) throws IOException {
        if (exists(filePath)) {
            Path path = Paths.get(filePath);
            if (!Files.isDirectory(path)) {
                Files.delete(path);
                return true;
            }
        }
        return false;
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
    }

}
