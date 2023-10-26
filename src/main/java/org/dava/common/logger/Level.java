package org.dava.common.logger;

public enum Level {

    ERROR(AnsiColor.RED.BOLD()),
    WARNING(AnsiColor.YELLOW.BOLD()),
    INFO(AnsiColor.BLUE.BOLD()),
    DEBUG(AnsiColor.GREEN.BOLD()),
    TRACE(AnsiColor.CYAN.BOLD());

    private AnsiColor ansiColor;

    Level(AnsiColor ansiColor) {
        this.ansiColor = ansiColor;
    }

    public boolean isLoggable(Level level) {
        return this.compareTo(level) >= 0;
    }

    public AnsiColor getAnsiColor() {
        return ansiColor;
    }

    public java.util.logging.Level map() {
        return switch(this) {
            case ERROR -> java.util.logging.Level.SEVERE;
            case WARNING -> java.util.logging.Level.WARNING;
            case INFO -> java.util.logging.Level.INFO;
            case DEBUG -> java.util.logging.Level.CONFIG;
            case TRACE -> java.util.logging.Level.FINEST;
        };
    }


}
