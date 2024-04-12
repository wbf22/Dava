package org.dava.core.common.logger;

import java.util.regex.Pattern;

public enum AnsiColor {
    //https://gist.github.com/JBlond/2fea43a3049b38287e5e9cefc87b2124

    RESET(0),
    BOXED(51),

    // Regular Colors
    BLACK(30),
    RED(31),
    GREEN(32),
    YELLOW(33),
    BLUE(34),
    PURPLE(35),
    CYAN(36),
    WHITE(37),

    // High Intensity Colors
    BLACK_HIGH_INTENSITY(90),
    RED_HIGH_INTENSITY(91),
    GREEN_HIGH_INTENSITY(92),
    YELLOW_HIGH_INTENSITY(93),
    BLUE_HIGH_INTENSITY(94),
    PURPLE_HIGH_INTENSITY(95),
    CYAN_HIGH_INTENSITY(96),
    WHITE_HIGH_INTENSITY(97),

    // Background Colors
    BLACK_BACKGROUND(40),
    RED_BACKGROUND(41),
    GREEN_BACKGROUND(42),
    YELLOW_BACKGROUND(43),
    BLUE_BACKGROUND(44),
    PURPLE_BACKGROUND(45),
    CYAN_BACKGROUND(46),
    WHITE_BACKGROUND(47),

    // High Intensity Background Colors
    BLACK_HIGH_INTENSITY_BACKGROUND(100),
    RED_HIGH_INTENSITY_BACKGROUND(101),
    GREEN_HIGH_INTENSITY_BACKGROUND(102),
    YELLOW_HIGH_INTENSITY_BACKGROUND(103),
    BLUE_HIGH_INTENSITY_BACKGROUND(104),
    PURPLE_HIGH_INTENSITY_BACKGROUND(105),
    CYAN_HIGH_INTENSITY_BACKGROUND(106),
    WHITE_HIGH_INTENSITY_BACKGROUND(107);




    private final int code;
    private int style = 0;

    AnsiColor(int code) {
        this.code = code;
    }

    public static String removeAnsiColors(String input) {
        if (input == null) return null;
        String ansiEscapeCodePattern = "\u001B\\[[;\\d]*[ -/]*[@-~]";
        Pattern pattern = Pattern.compile(ansiEscapeCodePattern);
        return pattern.matcher(input).replaceAll("");
    }

    public AnsiColor BOLD() {
        this.style = 1;
        return this;
    }

    public AnsiColor DIM() {
        this.style = 2;
        return this;
    }

    public AnsiColor ITALIC() {
        this.style = 3;
        return this;
    }

    public AnsiColor UNDERLINED() {
        this.style = 4;
        return this;
    }

    public AnsiColor BLINKING() {
        this.style = 5;
        return this;
    }

    public AnsiColor INVERTED() {
        this.style = 7;
        return this;
    }

    public AnsiColor HIDDEN() {
        this.style = 8;
        return this;
    }

    public AnsiColor STRIKE_THROUGH() {
        this.style = 9;
        return this;
    }

    public AnsiColor BOLD_UNDERLINE() {
        this.style = 21;
        return this;
    }


    @Override
    public String toString() {
        return build(code, style);
    }

    public static String build(int colorCode, int styleCode) {
        return "\u001B[" + styleCode + ";" + colorCode + "m";
    }

}
