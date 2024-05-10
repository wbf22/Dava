package org.dava.core.database.service.fileaccess;

import org.dava.core.database.service.operations.common.WritePackage;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class FileUtil {



    public void writeObjectToFile(String destinationPath, Object object) throws IOException {
        try ( ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(destinationPath)) ) {
            oos.writeObject(object);
        }

        
    }

    public <T> T readObjectFromFile(String filePath, Class<T> objectType) throws IOException {
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


    public String readFile(String filePath) throws IOException {
        return new String(readBytes(filePath), StandardCharsets.UTF_8);
    }

    public byte[] readBytes(String filePath) throws IOException {

        File file = new File(filePath);

        FileInputStream fileInputStream = new FileInputStream(file);

        byte[] fileContent = fileInputStream.readAllBytes();
        fileInputStream.close();

        return fileContent;
    }

    public String readFile(String filePath, long startByte, int numBytes) throws IOException {

        byte[] bytes = readBytes(filePath, startByte, numBytes);
        return (bytes != null)? new String(bytes, StandardCharsets.UTF_8) : null;
    }

    public byte[] readBytes(String filePath, long startByte, Integer numBytes) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(startByte); // Set the file pointer to the desired position

            int bytesToRead = (numBytes == null)? (int) raf.length() : numBytes;
            byte[] buffer = new byte[bytesToRead];
            int bytesRead = raf.read(buffer); // Read the specified number of bytes

            return (bytesRead != -1)? buffer : null;
        }

    }

    public List<Object> readBytes(String filePath, List<Long> startBytes, List<Long> numBytes) throws IOException {

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {

            List<Object> reads = new ArrayList<>();
            for (int i = 0; i < startBytes.size(); i++) {
                raf.seek( startBytes.get(i) ); // Set the file pointer to the desired position

                Long num = numBytes.get(i);

                byte[] buffer = new byte[ Math.toIntExact(num) ];
                int bytesRead = raf.read(buffer); // Read the specified number of bytes
                Object readBytes = (bytesRead != -1)? buffer : null;

                reads.add(
                    readBytes
                );
            }

            return reads;
        }


    }

    public String readLine(String filePath, long startByte) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        raf.seek(startByte); // Set the file pointer to the desired position

        String data = raf.readLine();

        raf.close();

        return data;
    }

    public void writeFile(String desitnationPath, String fileContents) throws IOException {

        FileOutputStream fileOutputStream;
        fileOutputStream = new FileOutputStream(desitnationPath);

        fileOutputStream.write(fileContents.getBytes(StandardCharsets.UTF_8));
        fileOutputStream.close();
        
    }

    public void writeBytes(String filePath, long position, byte[] data) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            // Move to the desired position in the file
            file.seek(position);

            // Write data at the current position
            file.write(data);
        }
    }

    public void changeCount(String filePath, long position, long change, int countByteLength) throws IOException {

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
    }

    /**
     * writes the writePackages.
     * Also updates null offsets in packages to the end of the table at insert
     */
    public void writeBytes(String filePath, List<WritePackage> writePackages) throws IOException {

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


    /**
     * writes the writePackages if they are within the length of the file
     * Also updates null offsets in packages to the end of the table at insert
     */
    public void writeBytesIfPossible(String filePath, List<WritePackage> writePackages) throws IOException {

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
    }

    public void writeFileAppend(String filePath, String data) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.seek(file.length());
            file.write(data.getBytes());
        }
    }

    public void writeBytesAppend(String filePath, byte[] data) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.seek(file.length());
            file.write(data);
        }
    }

    public void replaceFile(String filePath, byte[] data) throws IOException {

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength( 0 );
            file.write(data);
        }
    }

    public void replaceFile(String filePath, List<WritePackage> writePackages) throws IOException {

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
    }

    public void truncate(String filePath, Long newSizeInBytes) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength( newSizeInBytes );
        }
    }

    public void createDirectoriesIfNotExist(String directoryPath) throws IOException {

        Path path = Paths.get(directoryPath);
        Files.createDirectories(path);
    }

    public void moveFilesToDirectory(List<File> sourceFiles, String destinationDirectory) throws IOException {

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

    public void copyFilesToDirectory(List<File> sourceFiles, String destinationDirectory) throws IOException {

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

        
    }

    public boolean createFile(String filePath) throws IOException {
        
        return new File(filePath).createNewFile();
    }

    public boolean createFile(String filePath, byte[] content) throws IOException {

        File newFile = new File(filePath);
        boolean success = newFile.createNewFile();
        writeBytes(filePath, 0, content);


        
        return success;
    }

    public boolean renameFile(String oldFilePath, String newFilePath) {

        File oldFile = new File(oldFilePath);
        File newFile = new File(newFilePath);

        boolean success = false;
        if (oldFile.exists()) {
            success = oldFile.renameTo(newFile);
        }

        
        return success;
    }

    public boolean exists(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    public Long fileSize(String filePath) {
        
        File file = new File(filePath);
        return file.length();
    }

    public long popBytes(String filePath, int bytesToPop, List<Long> startBytes) throws IOException { //, boolean setFirst8BytesAsSizeOfThisFile

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

            return raf.length();
        }
    }

    public boolean deleteFile(String filePath) throws IOException {

        boolean success = false;
        if (exists(filePath)) {
            Path path = Paths.get(filePath);
            if (!Files.isDirectory(path)) {
                Files.delete(path);
                success = true;
            }
        }

        
        return success;
    }

    public boolean deleteFile(File file) throws IOException {

        boolean success = file.delete();

        
        return success;
    }

    public void deleteDirectory(String directoryPath) throws IOException {

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



    /*
           TODO action below require us to invalidate the entire cache in some of the methods above,
                decide if we should be smarter about which cache entries to invalidate.
     */

    public File[] listFiles(String path) {
        return new File(path).listFiles();
    }

    public File[] listFilesIfDirectory(String path) {
        File file = new File(path);
        if (file.isDirectory()) {
            return file.listFiles();
        }
        return null;
    }

    public List<File> getSubFolders(String path) {
        
        File[] files = listFiles(path);
        if (files != null && files.length > 0) {
            return Arrays.stream(files)
                .filter(File::isDirectory)
                .toList();
        }
        return new ArrayList<>();
    }

    public List<File> getSubFiles(String path) {
        
        File[] files = listFiles(path);
        if (files != null && files.length > 0) {
            return Arrays.stream(files)
                .filter(File::isFile)
                .toList();
        }
        return new ArrayList<>();
    }

    public List<File> getLeafFolders(String directory) {
        List<File> leaves = new ArrayList<>();
        List<File> files = List.of(new File(directory));
        do {
            files = files.parallelStream()
                .flatMap( file -> {
                    File[] subFiles = file.listFiles();
                    if (subFiles == null) {
                        leaves.add(file);
                        return new ArrayList<File>().stream();
                    }
                    
                    List<File> subDirs = Arrays.stream(
                        subFiles
                    )
                    .filter(File::isDirectory)
                    .toList();
                    if (subDirs.isEmpty())
                        leaves.add(file);
                    return subDirs.stream();
                })
                .toList();

        } while (!files.isEmpty());

        return leaves;
    }

    public List<File> getSubFoldersRecursive(String directory) {

        List<File> newFiles = Arrays.stream(
            new File(directory).listFiles()
        ).filter(File::isDirectory)
        .toList();

        List<File> files = new ArrayList<>(newFiles);

        while(!newFiles.isEmpty()) {
            newFiles = newFiles.parallelStream()
                .flatMap(file ->
                    Arrays.stream(
                        file.listFiles()
                    )
                )
                .filter(File::isDirectory)
                .toList();

            files.addAll(newFiles);
        }

        return files;
    }

    public long getSubFilesAndFolderCount(String directory) throws IOException {
        
        Path dir = Paths.get(directory);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            int count = 0;
            for (Path ignored : stream) {
                count++;
            }
            return count;
        }


    }


}
