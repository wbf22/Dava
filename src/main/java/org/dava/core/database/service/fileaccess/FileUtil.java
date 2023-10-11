package org.dava.core.database.service.fileaccess;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

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

    public static String readFile(String filePath, long startByte, int numBytes) throws IOException {
        byte[] bytes = readBytes(filePath, startByte, numBytes);
        return (bytes != null)? new String(bytes, StandardCharsets.UTF_8) : null;
    }

    public static byte[] readBytes(String filePath, long startByte, int numBytes) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        raf.seek(startByte); // Set the file pointer to the desired position

        byte[] buffer = new byte[numBytes];
        int bytesRead = raf.read(buffer); // Read the specified number of bytes

        byte[] data = (bytesRead != -1)? buffer : null;

        raf.close();

        return data;
    }

    public static String readLine(String filePath, long startByte) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        raf.seek(startByte); // Set the file pointer to the desired position

        String data = raf.readLine();

        raf.close();

        return data;
    }

    public static void createDirectoriesIfNotExist(String directoryPath) throws IOException {
        Path path = Paths.get(directoryPath);
        Files.createDirectories(path);
    }

    public static boolean createFile(String filePath) throws IOException {
        return new File(filePath).createNewFile();
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

    public static boolean exists(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    public static long fileSize(String filePath) {
        File file = new File(filePath);
        return file.length();
    }

    public static byte[] popRandomBytes(String filePath, int bytesToPop, Random random) throws IOException {
        File file = new File(filePath);

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            // get a random set of bytes
            long startByte = (
                random.nextLong( 0, raf.length()/bytesToPop )
            ) * bytesToPop;
            raf.seek(startByte);
            byte[] buffer = new byte[bytesToPop];
            int bytesRead = raf.read(buffer);
            byte[] randomData = (bytesRead != -1)? buffer : null;

            // get lastRow
            long endStartByte = raf.length() - bytesToPop;
            raf.seek(endStartByte);
            byte[] endBuffer = new byte[bytesToPop];
            int endBytesRead = raf.read(buffer);
            byte[] endData = (endBytesRead != -1)? endBuffer : null;

            // switch the two
            raf.seek(startByte);
            raf.write(endData);

            // truncate the end bytes off the file
            raf.setLength(startByte);

            return randomData;
        }
    }

    public static void deleteFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        Files.delete(path);
    }
}
