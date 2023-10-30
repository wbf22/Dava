package org.dava.core.systemtests;

import org.dava.common.logger.Logger;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

public class RandomTests {


    private static final Logger log = Logger.getLogger(RandomTests.class.getName());

    @Test
    void seeIfCertainCharactersArentInData() throws IOException {
        /*

                Pretty much anything less than 31 seems safe except for decimal 10 ('\n')

         */


        byte[] bytes = FileUtil.readBytes("db/Order/Order.csv");

        IntStream.range(0, bytes.length)
            .forEach( i -> {
                byte atI = bytes[i];
                if (atI < 31) {
                    log.info("found byte: " + atI);
                }
            });




    }

    @Test
    void testUTF_8_characters() {
//        byte[] bytes = new byte[]{0, 4};
//        String string = new String( bytes, StandardCharsets.UTF_8 );
//        log.print(
//            string
//        );
//        byte[] backOut = string.getBytes(StandardCharsets.UTF_8);
//        log.print(
//            new String( backOut, StandardCharsets.UTF_8 )
//        );


        int codePoint = 128512; // Unicode code point for a smiley face
        byte[] bytes = String.valueOf(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8);

        // Convert the byte array back to a string using UTF-8 encoding
        String str = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(str);
    }

}
