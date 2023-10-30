package org.dava.common;

import org.dava.common.logger.Level;
import org.dava.common.logger.Logger;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

class HashUtilTest {

    Logger log = Logger.getLogger(HashUtilTest.class.getName());


    @Test
    void test_for_collision() {
        Logger.setApplicationLogLevel(Level.DEBUG);

        Map<Long, String> values = new HashMap<>();
        int iterations = 10000000;
        IntStream.range(0, iterations)
            .forEach( i -> {
                String id = UUID.randomUUID().toString();
                long hash = HashUtil.hashString(id.getBytes());
                if (values.containsKey(hash)) {
                    log.info("Hash collision: " + hash + " " + id + " " + values.get(hash));
                }
                else {
                    values.put(hash, id);
                }


                if (i % 1000000 == 0) {
                    log.debug(i + "/" + iterations);
                }
            });
    }

    @Test
    void test_uuid_speed() {
        /*
            UUID is just a touch slower than my hash function, definitely good enough

                UUID: [INFO] 2023-10-27 11:31 12.73s -0600 [America/Denver] 4.72E-4ms
                HASH: [INFO] 2023-10-27 11:31 16.58s -0600 [America/Denver] 3.841E-4ms

         */


        int iterations = 10000000;
        Timer timer = Timer.start();
        IntStream.range(0, iterations)
            .forEach( i -> {
                String str = UUID.randomUUID().toString() + "bob is the best guy";
                UUID id = UUID.nameUUIDFromBytes(str.getBytes());
            });
        double timeForUUID = timer.getElapsed()/(1.0 * iterations);
        log.info(timeForUUID + "ms");


        timer = Timer.start();
        IntStream.range(0, iterations)
            .forEach( i -> {
                String str = UUID.randomUUID().toString() + "bob is the best guy";
                long id = HashUtil.hashString(str.getBytes());
            });
        double timeForHash = timer.getElapsed()/(1.0 * iterations);
        log.info(timeForHash + "ms");
    }


    @Test
    void test_for_collision_uuid() {
        Logger.setApplicationLogLevel(Level.DEBUG);

        Map<UUID, String> values = new HashMap<>();
        int iterations = 10000000;
        IntStream.range(0, iterations)
            .forEach( i -> {
                String str = UUID.randomUUID().toString() + "bob is the best guy";
                UUID id = UUID.nameUUIDFromBytes(str.getBytes());
                if (values.containsKey(id)) {
                    log.info("Hash collision: " + id + " " + str + " " + values.get(id));
                }
                else {
                    values.put(id, str);
                }


                if (i % 1000000 == 0) {
                    log.debug(i + "/" + iterations);
                }
            });
    }
}
