package org.dava.core.speedtests;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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


    public static void writeFile(String desitnationPath, String fileContents) throws IOException {
        FileOutputStream fileOutputStream;
        fileOutputStream = new FileOutputStream(desitnationPath);

        fileOutputStream.write(fileContents.getBytes(StandardCharsets.UTF_8));
        fileOutputStream.close();
    }


    public static void writeToOffset(String filePath, long offset, byte[] data) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.seek(offset);
            file.write(data);
//            file.writeInt();
        }
    }

    public static String readFile(String filePath) throws IOException {
        File file = new File(filePath);

        FileInputStream fileInputStream = new FileInputStream(file);

        byte[] fileContent = fileInputStream.readAllBytes();
        fileInputStream.close();

        return new String(fileContent, StandardCharsets.UTF_8);
    }

    public static String readFile(String filePath, long startByte, int numBytes) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        raf.seek(startByte); // Set the file pointer to the desired position

        byte[] buffer = new byte[numBytes];
        int bytesRead = raf.read(buffer); // Read the specified number of bytes

        String data = (bytesRead != -1)? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : null;

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

    /**
     * WARNING: this method is pretty inefficient compared to readFile() provided start byte.
     *
     * Actually faster to run readFile(path) above and do a split string by '\n' char,
     * if (lineNumber > fileLines/2)
     *
     */
    public static String readLineFromFile(String filePath, int lineNumber, boolean haveReadWarning) throws IOException {
        if (!haveReadWarning) {
            throw new RuntimeException("This method is less efficient. See docs before use");
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int currentLine = 0;

            while ((line = reader.readLine()) != null) {
                currentLine++;
                if (currentLine == lineNumber) {
                    return line;
                }
            }
            throw new RuntimeException("File only had " + currentLine + " lines");
        }
    }


    public static void deleteFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        Files.delete(path);
    }
}
