package org.dava.core.database.service.caching;

@FunctionalInterface
public interface CheckedSupplier<T, E extends Exception> {
    T get() throws E;
}
