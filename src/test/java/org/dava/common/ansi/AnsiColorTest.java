package org.dava.common.ansi;

import org.dava.common.logger.AnsiColor;
import org.junit.jupiter.api.Test;

import static org.dava.common.logger.AnsiColor.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnsiColorTest {


    @Test
    void printAllCombos() {
        for (int code = 30; code < 200; code++ ) {
            for (int style = 0; style < 10; style++ ) {
                System.out.println(
                    AnsiColor.build(code, style) + code + ":" + style + RESET
                );
            }
        }
        assertTrue(true);
    }

    @Test
    void printColor() {
        int code = 1;
        int style = 32;
        int code2 = 31;
        int style2 = 1;


        System.out.println(
                AnsiColor.build(code, style) + code + ":" + style + RESET
        );

        System.out.println(
                AnsiColor.build(code2, style2) + code2 + ":" + style2 + RESET
        );


        AnsiColor color = BOXED.BOLD();
        String text = "1:1";
        System.out.println(
                color + text + RESET
        );
        assertTrue(true);
    }
}
