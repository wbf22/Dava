package org.dava.core.common;

public class Bundle <T,U> {

    private T first;
    private U second;


    public Bundle(T first, U second) {
        this.first = first;
        this.second = second;
    }


    public T getFirst() {
        return first;
    }

    public void setFirst(T first) {
        this.first = first;
    }

    public U getSecond() {
        return second;
    }

    public void setSecond(U second) {
        this.second = second;
    }
}
