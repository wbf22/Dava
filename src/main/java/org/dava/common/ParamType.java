package org.dava.common;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class ParamType<T> {
    private final java.lang.reflect.Type type;

    public ParamType() {
        Type superClass = getClass().getGenericSuperclass();
        if (superClass instanceof Class) {
            throw new IllegalArgumentException("Missing type parameter.");
        }
        this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }


    public Type getType() {
        return this.type;
    }
}

