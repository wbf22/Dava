package org.dava.core.database.objects.database.structure;

/**
 * Mode for table optimization. Organized by
 * (slowest insertion / fastest query or access) to (fastest insertion / slowest query or access)
 * The modes allow you to optimize your table for these qualities, changing how the table
 * is managed. Certain modes collect more meta data to speed up queries, while others collect
 * less and because of that inserts/updates are faster.
 */
public enum Mode {
    /**
     * Trades slower insertion/update times for faster queries.
     */
    INDEX_ALL,

    /**
     * Balance between insertion/update time and queries. This mode
     * only indexes columns that are queried.
     */
    STORAGE_SENSITIVE,

    /**
     * Fast insertion and access by the table's primary key. Ideal if
     * you won't perform complex queries, or want to specify which
     * columns should be indexed.
     */
    MANUAL,

    /**
     * Lighting insertion, very slow access. Basically just operates
     * on the table .csv file, with no table meta data. Only mode where
     * it is safe to modify the table .csv file directly. Ideal for very
     * small tables were you'd like to directly modify the csv file. Or if
     * you want instant insertion and don't mind slow access (weird)
     */
    LIGHT;
}
