package org.dava.core.database.service.objects;

import java.io.IOException;

@FunctionalInterface
public interface IORunnable {
    void run() throws IOException;
}
