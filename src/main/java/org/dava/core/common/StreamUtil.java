package org.dava.core.common;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class StreamUtil {


    public static <T, R> Stream<R> enumerate(List<T> list, BiFunction<Integer, T, R> lambda) {
        return IntStream.range(0, list.size())
            .mapToObj(i ->
                lambda.apply(i, list.get(i))
            );
    }

    public static <T> void enumerate(List<T> list, BiConsumer<Integer, T> lambda) {
        IntStream.range(0, list.size())
            .forEach( i -> lambda.accept(i, list.get(i)) );
    }

}
