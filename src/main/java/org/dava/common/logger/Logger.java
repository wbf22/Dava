package org.dava.common.logger;



import org.dava.common.ansi.AnsiColor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.dava.common.ansi.AnsiColor.RESET;

public class Logger {

    private static final String LOG_PROPERTY_TAG = "LOG_LEVEL";
    private static List<String> logToFiles = new ArrayList<>();
    private static Level logLevel = Level.INFO;
    private String name;
    private Formatter formatter = new DefaultFormatter();
    private Logger(String loggerName) {
        this.name = loggerName;
    }



    /*
        public methods
     */

    public void print(String message) {
        System.out.println( message );
    }

    public void space() {
        print("");
        logToFiles.forEach(path ->
                appendToFile(
                        path,
                        "\n"
                ));
    }

    public void error(String message) {
        log(message, Level.ERROR);
    }

    public void warning(String message) {
        log(message, Level.WARNING);
    }

    public void info(String message) {
        log(message, Level.INFO);
    }

    public void debug(String message) {
        log(message, Level.DEBUG);
    }

    public void trace(String message) {
        log(message, Level.TRACE);
    }



    /*
        private methods
     */
    private void log(String message, Level level) {
        if (logLevel.isLoggable(level)) {
            String formattedMessage = formatMessage(level, message);
            print(formattedMessage);

            logToFiles.forEach(path ->
                    appendToFile(
                        path,
                        AnsiColor.removeAnsiColors(formattedMessage) + "\n"
                    ));
        }
    }

    private void appendToFile(String filePath, String message) {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.seek(file.length());
            file.write(message.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            String errMsg = formatMessage(Level.ERROR, "Failure logging to log file: " + filePath);
            print(errMsg);
        }
    }

    private String formatMessage(Level level, String message) {
        String levelTag = "[" + level.getAnsiColor() +  level.name() + RESET + "] ";
        return levelTag + formatter.format(message);
    }




    /*
        global config methods
     */
    public static void logToFile(String path) {
        logToFiles.add(path);
    }

    public static void setApplicationLogLevel(Level level) {
        logLevel = level;
        getLogger("Logger").info("Logger Level is set to " + logLevel.name());
    }



    /*
        individual logger config methods
     */
    public static Logger getLogger(String loggerName) {
        return new Logger(loggerName);
    }

    public Logger withFormatter(Formatter customFormatter) {
        this.formatter = customFormatter;
        return this;
    }

}
