package org.dava.core.common;

public class Timer {

    private long startTime;


    public Timer(long startTime) {
        this.startTime = startTime;
    }

    public static Timer start() {
        return new Timer(System.currentTimeMillis());
    }


    public void printRestart() {
        printRestart("");
    }

    public void printRestart(String message) {
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println(message + " " + elapsed + "ms");
        startTime = System.currentTimeMillis();
    }

    public long getElapsed() {
        return System.currentTimeMillis() - startTime;
    }



}
