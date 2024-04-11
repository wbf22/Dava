package org.dava;

import java.util.logging.Logger;

import org.dava.external.DavaId;
import org.junit.jupiter.api.Test;

public class DavaTest {


    @Test
    public void test() {
        Logger logger = Logger.getLogger(DavaTest.class.getName());

        logger.info(
            DavaId.randomId("ord", 16, 8, true)
        );
        logger.info(
            DavaId.randomId("ord", 16, 8, false)
        );

        //ord_ef3fa803-425c-4a12-9a57-b9346a044d24
        //ord_91BBDFC8-E28B-44FE-8663-CDFE6F954082
    }
    
}
