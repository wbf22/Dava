package org.dava.random.systemtests;

import org.dava.core.common.Timer;
import org.dava.core.common.logger.Logger;
import org.dava.core.database.objects.dates.ZonedDate;
import org.dava.core.database.service.caching.Cache;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class RandomTests {


    private static final Logger log = Logger.getLogger(RandomTests.class.getName());

    FileUtil fileUtil = new FileUtil();

    @Test
    void seeIfCertainCharactersArentInData() throws IOException {
        /*

                Pretty much anything less than 31 seems safe except for decimal 10 ('\n')

         */

        byte[] bytes = fileUtil.readBytes("db/Order/Order.csv");

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

    @Test
    void stringBuilding_vs_concat() {
        /*
            TEST RESULTS:
                35ms
                20ms
                33ms

            using stringbuilder is faster, even for short strings. Also don't use the constructor for stringbuilder, just
            append().
         */

        int ITERATIONS = 1000000;

        String base = "this is a base string";
        String addition = " and here is the addition";
        String terminator = "/";

        Timer timer = Timer.start();
        IntStream.range(0, ITERATIONS)
            .forEach( i -> {
                String s = base + addition + terminator + terminator;
            });
        timer.printRestart();

        timer = Timer.start();
        IntStream.range(0, ITERATIONS)
            .forEach( i -> {
                String s = new StringBuilder().append(base).append(addition).append(terminator).append(terminator).toString();
            });
        timer.printRestart();

        timer = Timer.start();
        IntStream.range(0, ITERATIONS)
            .forEach( i -> {
                String s = new StringBuilder(base).append(addition).append(terminator).append(terminator).toString();
            });
        timer.printRestart();

    }


    @Test
    void test_random_with_seed() {
        /*
            TEST RESULTS:
                35ms
                20ms
                33ms

            using stringbuilder is faster, even for short strings. Also don't use the constructor for stringbuilder, just
            append().
         */

        int SEED = 123456789;

        Random random = new Random(SEED);
        IntStream.range(0, 3)
            .forEach( i -> {
                log.info(String.valueOf(random.nextInt()));
            });


        Random randomN = new Random(SEED);
        IntStream.range(0, 3)
            .forEach( i -> {
                log.info(String.valueOf(randomN.nextInt()));
            });
    }


    @Test
    void deterministic_uuid() {
        IntStream.range(0, 10)
            .forEach(i ->
                log.info(UUID.nameUUIDFromBytes(String.valueOf(i).getBytes()).toString())
            );

        log.space();


        IntStream.range(0, 10)
            .forEach(i ->
                log.info(UUID.nameUUIDFromBytes(String.valueOf(i).getBytes()).toString())
            );
    }


    @Test
    void deleteDirectory() throws IOException {
        fileUtil.deleteDirectory("db" + "/Order");
    }


    @Test
    void testStringComparingLongVsShort() {

        /*
            TEST RESULTS:
                4ms
                5ms

            Comparing a short string vs a long string is almost no difference in time
         */

         int ITERATIONS = 100000000;

         String shortStr = "ord_dfasdfasd";
         String shortStr2 = "ord_dfasdfass";
         String longStr = "ddddddasdlkfasd;glkawe;loijg;asdoifkja;sdlfkajsdfasdfasdfddddddasdlkfasd;glkawe;loijg;asdoifkja;sdlfkajsdfasdfasdfddddddasdlkfasd;glkawe;loijg;asdoifkja;sdlfkajsdfasdfasdf";
         String longStr2 = "ddddddasdlkfasd;glkawe;loijg;asdoifkja;sdlfkajsdfasdfasdfddddddasdlkfasd;glkawe;loijg;asdoifkja;sdlfkajsdfasdfasdfddddddasdlkfasd;glkawe;loijg;asdoifkja;sdlfkajsdfasdfasdd";
 
         Timer timer = Timer.start();
         IntStream.range(0, ITERATIONS)
             .forEach( i -> {
                boolean equal = shortStr.equals(shortStr2);
             });
         timer.printRestart();
 
         timer = Timer.start();
         IntStream.range(0, ITERATIONS)
             .forEach( i -> {
                boolean equal = longStr.equals(longStr2);
             });
         timer.printRestart();

    }



    @Test
    void test_cache_hash_function_overhead() throws InterruptedException {
        /*
            TEST RESULTS:
                33ms
                66ms
                3ms

            Just concatenating strings is faster than using the cache hash function, but both are
            super fast that there isn't any real savings.

            With a query with say 10 cache hits it's only 1ms of overhead.

            If we're caching on the query level though, we'd only have one cache hit so that'd be 
            sub 1ms of overhead.
         */

        int ITERATIONS = 100000;

        String operationName = "operationName";
        String param1 = "param1";
        String param2 = "param2";
        String param3 = "param3";
        List<String> paramList = List.of(param1, param2, param3);

        // warm up
        IntStream.range(0, ITERATIONS)
            .forEach( i -> {
                String hash = Cache.hash(operationName, param1);
            });

        Thread.sleep(20);

        // normal
        Timer timer = Timer.start();
        IntStream.range(0, ITERATIONS)
            .forEach( i -> {
                String hash = Cache.hash(operationName, param1);
            });
        timer.printRestart();


        // with list
        timer = Timer.start();
        IntStream.range(0, ITERATIONS)
            .forEach( i -> {
                String hash = Cache.hash(operationName, Cache.hashList(paramList));
            });
        timer.printRestart();



        // plain string
        timer = Timer.start();
        IntStream.range(0, ITERATIONS)
            .forEach( i -> {
                String hash = new StringBuilder().append(operationName).append(param1).toString();
            });
        timer.printRestart();

    }

}
