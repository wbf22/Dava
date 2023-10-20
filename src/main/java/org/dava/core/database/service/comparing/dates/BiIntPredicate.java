package org.dava.core.database.service.comparing.dates;


@FunctionalInterface
interface BiIntPredicate {
    boolean test(int x, int y);
}

