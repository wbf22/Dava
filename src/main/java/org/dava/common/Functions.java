package org.dava.common;

public interface Functions {

    @FunctionalInterface
    interface TriFunction<T, U, S, R> {
        R apply(T t, U u, S s);
    }
}
