package org.dava.core.database.service.fileaccess;

import java.io.IOException;

@FunctionalInterface
interface IOSupplier<T> extends CheckedSupplier<T, IOException> {
    T get() throws IOException;
}
