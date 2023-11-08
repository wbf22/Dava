package org.dava.core.database.service.fileaccess;

@FunctionalInterface
interface CheckedSupplier<T, E extends Exception> {
    T get() throws E;
}
