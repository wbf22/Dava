package org.dava.common.logger;


public class DefaultFormatter implements Formatter {


    @Override
    public String format(String message) {
        return getTimeStamp() + " " + message;
    }

    @Override
    public String getTimeStamp() {
        return Formatter.super.getTimeStamp();
    }
}
