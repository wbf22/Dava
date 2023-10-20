package org.dava.common;

public class Timer {

    private long startTime;


    public Timer(long startTime) {
        this.startTime = startTime;
    }

    public static Timer start() {
        return new Timer(System.currentTimeMillis());
    }


    public void printRestart() {
        System.out.println(System.currentTimeMillis() - startTime + "ms");
        startTime = System.currentTimeMillis();
    }

    public long getElapsed() {
        return System.currentTimeMillis() - startTime;
    }



}
